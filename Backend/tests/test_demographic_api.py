import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.demographic import DemographicFiles, DemographicRow

INGESTION_API = "/api/v1/ingestion"
DEMOGRAPHIC_API = "/api/v1/demographic"
P1_STR = "00000000-0000-0000-0000-000000000001"


async def _create_corpus(client, name: str = "Corpus") -> str:
    response = await client.post(
        f"{INGESTION_API}/corpora",
        json={"project_id": P1_STR, "name": name},
    )
    assert response.status_code == 201
    return response.json()["data"]["id"]


async def _upload_csv(client, corpus_id: str, filename: str, content: bytes, content_type: str = "application/octet-stream"):
    return await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/upload",
        files={"file": (filename, content, content_type)},
    )


async def test_demographic_csv_valid_dynamic_columns_and_confirm_persists_rows(client, db_engine):
    corpus_id = await _create_corpus(client, "Corpus A")

    csv_content = (
        "username;age;gender;department\n"
        "user_a;34;female;engineering\n"
    ).encode("utf-8")

    upload = await _upload_csv(client, corpus_id, "demographics.csv", csv_content)
    assert upload.status_code == 201
    upload_body = upload.json()
    assert upload_body["success"] is True
    assert upload_body["data"]["name"] == "demographics"
    assert upload_body["data"]["preview"]["columns_detected"] == 4
    assert upload_body["data"]["preview"]["rows_detected"] == 1

    import_id = upload_body["data"]["import_id"]
    confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": import_id, "confirm": True},
    )
    assert confirm.status_code == 201
    confirm_body = confirm.json()
    assert confirm_body["success"] is True
    assert confirm_body["data"]["name"] == "demographics"
    assert confirm_body["data"]["rows_created"] == 1

    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        file_row = (
            await session.execute(select(DemographicFiles).where(DemographicFiles.id == uuid.UUID(import_id)))
        ).scalar_one()
        stored_row = (
            await session.execute(
                select(DemographicRow).where(DemographicRow.demographic_file_id == uuid.UUID(import_id))
            )
        ).scalar_one()

    assert file_row.corpus_id == uuid.UUID(corpus_id)
    assert stored_row.interviewee_id == "user_a"
    assert stored_row.data == {"age": "34", "gender": "female", "department": "engineering"}
    assert "username" not in stored_row.data


async def test_demographic_csv_missing_values_are_accepted(client, db_engine):
    corpus_id = await _create_corpus(client, "Corpus Missing Values")

    csv_content = (
        "username;age;gender;income_band\n"
        "user_b;;non-binary;\n"
    ).encode("utf-8")
    upload = await _upload_csv(client, corpus_id, "missing_values.csv", csv_content)
    assert upload.status_code == 201
    assert upload.json()["success"] is True

    import_id = upload.json()["data"]["import_id"]
    confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": import_id, "confirm": True},
    )
    assert confirm.status_code == 201
    assert confirm.json()["success"] is True

    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        stored_row = (
            await session.execute(
                select(DemographicRow).where(DemographicRow.demographic_file_id == uuid.UUID(import_id))
            )
        ).scalar_one()
    assert stored_row.data == {"age": "", "gender": "non-binary", "income_band": ""}


async def test_demographic_upload_requires_interviewee_id_and_at_least_one_demographic_column(client):
    corpus_id = await _create_corpus(client, "Corpus Missing Columns")

    missing_id_column = "age,gender\n34,female\n".encode("utf-8")
    response_missing_id = await _upload_csv(client, corpus_id, "missing_id.csv", missing_id_column)
    assert response_missing_id.status_code == 201
    assert response_missing_id.json()["success"] is False
    assert "must include 'username' column" in response_missing_id.json()["meta"]["detail"]

    only_id_column = "username\nuser_c\n".encode("utf-8")
    response_only_id = await _upload_csv(client, corpus_id, "only_id.csv", only_id_column)
    assert response_only_id.status_code == 201
    assert response_only_id.json()["success"] is False
    assert "at least 2 columns" in response_only_id.json()["meta"]["detail"]


async def test_demographic_upload_validates_malformed_csv_row_and_reports_clear_error(client):
    corpus_id = await _create_corpus(client, "Corpus Malformed")

    malformed = (
        "username;age;gender\n"
        "user_d;29;male;unexpected\n"
    ).encode("utf-8")
    response = await _upload_csv(client, corpus_id, "malformed.csv", malformed)

    assert response.status_code == 201
    body = response.json()
    assert body["success"] is False
    assert "malformed CSV row" in body["meta"]["detail"]


async def test_demographic_upload_rejects_non_csv_extension_even_with_csv_mime_variants(client):
    corpus_id = await _create_corpus(client, "Corpus Extensions")
    content = "username;segment\nuser_e;A\n".encode("utf-8")

    ok_response = await _upload_csv(
        client,
        corpus_id,
        "demo.csv",
        content,
        content_type="application/octet-stream",
    )
    assert ok_response.status_code == 201
    assert ok_response.json()["success"] is True

    bad_response = await _upload_csv(
        client,
        corpus_id,
        "demo.txt",
        content,
        content_type="text/csv",
    )
    assert bad_response.status_code == 201
    assert bad_response.json()["success"] is False
    assert "Unsupported file extension" in bad_response.json()["meta"]["detail"]


async def test_demographic_upload_rejects_duplicate_username(client):
    corpus_id = await _create_corpus(client, "Corpus Dup Username")
    first = "username;role\nuser_f;participant\n".encode("utf-8")
    second = "username;role\nuser_f;participant\n".encode("utf-8")

    first_response = await _upload_csv(client, corpus_id, "first.csv", first)
    assert first_response.status_code == 201
    assert first_response.json()["success"] is True
    first_import_id = first_response.json()["data"]["import_id"]
    first_confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": first_import_id, "confirm": True},
    )
    assert first_confirm.status_code == 201
    assert first_confirm.json()["success"] is True

    response = await _upload_csv(client, corpus_id, "second.csv", second)
    assert response.status_code == 201
    assert response.json()["success"] is False
    assert "username already exists" in response.json()["meta"]["detail"]


async def test_demographic_upload_allows_same_username_in_different_corpora(client):
    corpus_a = await _create_corpus(client, "Corpus A")
    corpus_b = await _create_corpus(client, "Corpus B")
    csv_same_user = "username;role\nshared_user;participant\n".encode("utf-8")

    upload_a = await _upload_csv(client, corpus_a, "a.csv", csv_same_user)
    assert upload_a.status_code == 201
    assert upload_a.json()["success"] is True
    confirm_a = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_a}/confirm",
        params={"import_id": upload_a.json()["data"]["import_id"], "confirm": True},
    )
    assert confirm_a.status_code == 201
    assert confirm_a.json()["success"] is True

    upload_b = await _upload_csv(client, corpus_b, "b.csv", csv_same_user)
    assert upload_b.status_code == 201
    assert upload_b.json()["success"] is True
    confirm_b = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_b}/confirm",
        params={"import_id": upload_b.json()["data"]["import_id"], "confirm": True},
    )
    assert confirm_b.status_code == 201
    assert confirm_b.json()["success"] is True


async def test_demographic_confirm_second_attempt_fails_after_successful_confirm(client):
    corpus_id = await _create_corpus(client, "Corpus Confirm")
    csv_content = "username;country\nuser_g;DE\n".encode("utf-8")

    upload = await _upload_csv(client, corpus_id, "confirm.csv", csv_content)
    import_id = upload.json()["data"]["import_id"]

    first_confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": import_id, "confirm": True},
    )
    assert first_confirm.status_code == 201
    assert first_confirm.json()["success"] is True

    second_confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": import_id, "confirm": True},
    )
    assert second_confirm.status_code == 201
    assert second_confirm.json()["success"] is False
    assert "No pending upload found" in second_confirm.json()["meta"]["detail"]


async def test_demographic_name_unique_within_corpus(client, db_engine):
    corpus_id = await _create_corpus(client, "Corpus Names")
    csv_content_a = "username;country\nuser_h;DE\n".encode("utf-8")
    csv_content_b = "username;country\nuser_i;AT\n".encode("utf-8")

    first_upload = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/upload",
        data={"name": "participants"},
        files={"file": ("a.csv", csv_content_a, "application/octet-stream")},
    )
    first_import_id = first_upload.json()["data"]["import_id"]
    first_confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": first_import_id, "confirm": True},
    )
    assert first_confirm.json()["data"]["name"] == "participants"

    second_upload = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/upload",
        data={"name": "participants"},
        files={"file": ("b.csv", csv_content_b, "application/octet-stream")},
    )
    second_import_id = second_upload.json()["data"]["import_id"]
    second_confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": second_import_id, "confirm": True},
    )
    assert second_confirm.json()["data"]["name"] == "participants (2)"

    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        rows = (
            await session.execute(
                select(DemographicFiles).where(DemographicFiles.corpus_id == uuid.UUID(corpus_id))
            )
        ).scalars().all()
    names = sorted(row.name for row in rows)
    assert names == ["participants", "participants (2)"]


async def test_demographic_name_can_repeat_across_corpora(client):
    corpus_a = await _create_corpus(client, "Corpus A")
    corpus_b = await _create_corpus(client, "Corpus B")
    csv_a = "username;age\nuser_j;31\n".encode("utf-8")
    csv_b = "username;age\nuser_k;29\n".encode("utf-8")

    upload_a = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_a}/upload",
        data={"name": "shared-name"},
        files={"file": ("a.csv", csv_a, "application/octet-stream")},
    )
    upload_b = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_b}/upload",
        data={"name": "shared-name"},
        files={"file": ("b.csv", csv_b, "application/octet-stream")},
    )

    confirm_a = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_a}/confirm",
        params={"import_id": upload_a.json()["data"]["import_id"], "confirm": True},
    )
    confirm_b = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_b}/confirm",
        params={"import_id": upload_b.json()["data"]["import_id"], "confirm": True},
    )
    assert confirm_a.json()["data"]["name"] == "shared-name"
    assert confirm_b.json()["data"]["name"] == "shared-name"


async def _upload_and_confirm_named_csv(client, corpus_id: str, name: str, csv_content: bytes) -> str:
    upload = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/upload",
        data={"name": name},
        files={"file": (f"{name}.csv", csv_content, "application/octet-stream")},
    )
    assert upload.status_code == 201
    assert upload.json()["success"] is True
    import_id = upload.json()["data"]["import_id"]
    confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": import_id, "confirm": True},
    )
    assert confirm.status_code == 201
    assert confirm.json()["success"] is True
    return import_id


async def test_list_demographic_files_with_total_rows(client):
    corpus_id = await _create_corpus(client, "Corpus File List")

    csv_a = (
        "username;group\n"
        "user_l;A\n"
        "user_m;B\n"
    ).encode("utf-8")
    csv_b = (
        "username;group\n"
        "user_n;C\n"
    ).encode("utf-8")

    await _upload_and_confirm_named_csv(client, corpus_id, "batch-a", csv_a)
    await _upload_and_confirm_named_csv(client, corpus_id, "batch-b", csv_b)

    response = await client.get(f"{DEMOGRAPHIC_API}/{corpus_id}/files", params={"page": 1, "page_size": 20})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["meta"]["total"] == 2
    files = body["data"]["items"]
    by_name = {f["name"]: f for f in files}
    assert by_name["batch-a"]["rows_total"] == 2
    assert by_name["batch-b"]["rows_total"] == 1


async def test_list_demographic_rows_pagination_and_file_filter(client):
    corpus_id = await _create_corpus(client, "Corpus Row List")

    csv_a = (
        "username;group\n"
        "user_o;A\n"
        "user_p;B\n"
    ).encode("utf-8")
    csv_b = "username;group\nuser_q;C\n".encode("utf-8")

    await _upload_and_confirm_named_csv(client, corpus_id, "batch-a", csv_a)
    await _upload_and_confirm_named_csv(client, corpus_id, "batch-b", csv_b)

    files_resp = await client.get(f"{DEMOGRAPHIC_API}/{corpus_id}/files")
    files = files_resp.json()["data"]["items"]
    file_a_id = next(f["id"] for f in files if f["name"] == "batch-a")

    page_1 = await client.get(f"{DEMOGRAPHIC_API}/{corpus_id}/rows", params={"page": 1, "page_size": 2})
    assert page_1.status_code == 200
    page_1_body = page_1.json()["data"]
    assert page_1_body["meta"]["total"] == 3
    assert len(page_1_body["items"]) == 2

    filtered = await client.get(
        f"{DEMOGRAPHIC_API}/{corpus_id}/rows",
        params={"demographic_file_id": file_a_id, "page": 1, "page_size": 10},
    )
    assert filtered.status_code == 200
    filtered_body = filtered.json()["data"]
    assert filtered_body["meta"]["total"] == 2
    assert all(row["demographic_file_id"] == file_a_id for row in filtered_body["items"])

    other_corpus = await _create_corpus(client, "Other Corpus")
    other_csv = "username;group\nuser_r;Z\n".encode("utf-8")
    await _upload_and_confirm_named_csv(client, other_corpus, "other-batch", other_csv)
    other_files_resp = await client.get(f"{DEMOGRAPHIC_API}/{other_corpus}/files")
    other_file_id = other_files_resp.json()["data"]["items"][0]["id"]

    wrong_filter = await client.get(
        f"{DEMOGRAPHIC_API}/{corpus_id}/rows",
        params={"demographic_file_id": other_file_id},
    )
    assert wrong_filter.status_code == 200
    assert wrong_filter.json()["success"] is False
    assert "does not belong to corpus" in wrong_filter.json()["meta"]["detail"]
