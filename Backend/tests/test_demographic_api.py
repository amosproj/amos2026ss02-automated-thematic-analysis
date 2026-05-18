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


async def _create_document(client, corpus_id: str, title: str = "Doc", text: str = "Some transcript text") -> str:
    response = await client.post(
        f"{INGESTION_API}/corpora/{corpus_id}/documents/bulk",
        json={"documents": [{"title": title, "text": text}]},
    )
    assert response.status_code == 201

    doc_response = await client.get(f"{INGESTION_API}/corpora/{corpus_id}/documents")
    assert doc_response.status_code == 200
    return doc_response.json()["data"]["items"][0]["id"]


async def _upload_csv(client, corpus_id: str, filename: str, content: bytes, content_type: str = "application/octet-stream"):
    return await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/upload",
        files={"file": (filename, content, content_type)},
    )


async def test_demographic_csv_valid_dynamic_columns_and_confirm_persists_rows(client, db_engine):
    corpus_id = await _create_corpus(client, "Corpus A")
    document_id = await _create_document(client, corpus_id, "Interview A", "hello world")

    csv_content = (
        "corpus_document_id,age,gender,department\n"
        f"{document_id},34,female,engineering\n"
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
    assert stored_row.corpus_document_id == uuid.UUID(document_id)
    assert stored_row.data == {"age": "34", "gender": "female", "department": "engineering"}
    assert "corpus_document_id" not in stored_row.data


async def test_demographic_csv_missing_values_are_accepted(client, db_engine):
    corpus_id = await _create_corpus(client, "Corpus Missing Values")
    document_id = await _create_document(client, corpus_id)

    csv_content = (
        "corpus_document_id,age,gender,income_band\n"
        f"{document_id},,non-binary,\n"
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
    document_id = await _create_document(client, corpus_id)

    missing_id_column = "age,gender\n34,female\n".encode("utf-8")
    response_missing_id = await _upload_csv(client, corpus_id, "missing_id.csv", missing_id_column)
    assert response_missing_id.status_code == 201
    assert response_missing_id.json()["success"] is False
    assert "must include 'corpus_document_id' column" in response_missing_id.json()["meta"]["detail"]

    only_id_column = f"corpus_document_id\n{document_id}\n".encode("utf-8")
    response_only_id = await _upload_csv(client, corpus_id, "only_id.csv", only_id_column)
    assert response_only_id.status_code == 201
    assert response_only_id.json()["success"] is False
    assert "at least 2 columns" in response_only_id.json()["meta"]["detail"]


async def test_demographic_upload_validates_malformed_csv_row_and_reports_clear_error(client):
    corpus_id = await _create_corpus(client, "Corpus Malformed")
    document_id = await _create_document(client, corpus_id)

    malformed = (
        "corpus_document_id,age,gender\n"
        f"{document_id},29,male,unexpected\n"
    ).encode("utf-8")
    response = await _upload_csv(client, corpus_id, "malformed.csv", malformed)

    assert response.status_code == 201
    body = response.json()
    assert body["success"] is False
    assert "malformed CSV row" in body["meta"]["detail"]


async def test_demographic_upload_rejects_non_csv_extension_even_with_csv_mime_variants(client):
    corpus_id = await _create_corpus(client, "Corpus Extensions")
    document_id = await _create_document(client, corpus_id)
    content = f"corpus_document_id,segment\n{document_id},A\n".encode("utf-8")

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


async def test_demographic_upload_rejects_document_ids_from_another_corpus(client):
    corpus_a = await _create_corpus(client, "Corpus A")
    corpus_b = await _create_corpus(client, "Corpus B")
    doc_b = await _create_document(client, corpus_b, "Interview B", "text")

    cross_corpus_csv = (
        "corpus_document_id,role\n"
        f"{doc_b},participant\n"
    ).encode("utf-8")
    response = await _upload_csv(client, corpus_a, "cross_corpus.csv", cross_corpus_csv)
    assert response.status_code == 201
    assert response.json()["success"] is False
    assert "not present in corpus" in response.json()["meta"]["detail"]


async def test_demographic_confirm_second_attempt_fails_after_successful_confirm(client):
    corpus_id = await _create_corpus(client, "Corpus Confirm")
    document_id = await _create_document(client, corpus_id)
    csv_content = f"corpus_document_id,country\n{document_id},DE\n".encode("utf-8")

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
    document_id = await _create_document(client, corpus_id)
    csv_content = f"corpus_document_id,country\n{document_id},DE\n".encode("utf-8")

    first_upload = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/upload",
        data={"name": "participants"},
        files={"file": ("a.csv", csv_content, "application/octet-stream")},
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
        files={"file": ("b.csv", csv_content, "application/octet-stream")},
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
    doc_a = await _create_document(client, corpus_a)
    doc_b = await _create_document(client, corpus_b)
    csv_a = f"corpus_document_id,age\n{doc_a},31\n".encode("utf-8")
    csv_b = f"corpus_document_id,age\n{doc_b},29\n".encode("utf-8")

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
