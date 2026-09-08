# OpenRouter Models

OpenRouter is available as a first-class LLM provider in the Home-page provider
selector. The supplied profile uses:

```text
Chat model:      openai/gpt-5.6-sol
Embedding model: openai/text-embedding-3-small
Base URL:        https://openrouter.ai/api/v1
```

`openai/gpt-5.6-sol` is OpenRouter's provider-qualified identifier for OpenAI's
GPT-5.6 Sol model.

## Enable OpenRouter

1. Add an OpenRouter API key to `Backend/.env`:

   ```text
   LLM_API_KEY_OPENROUTER=<your_openrouter_api_key>
   ```

2. Recreate the API container so Compose reloads the environment:

   ```bash
   docker compose up -d --force-recreate api
   ```

   Alternatively, run `./teardown.sh` and then `./setup.sh` from the repository
   root. This preserves the database volume.

3. Open the Home page and select **OpenRouter** under **LLM Provider**.

Transcripts and generated code text are sent to OpenRouter and the upstream
model provider when this option is active. Check your OpenRouter account's model
access, privacy settings, limits, and prices before processing research data.

## Change The OpenRouter Model

The OpenRouter provider profile represents one configured chat model. To switch
that profile to another model, copy its exact provider-qualified model ID from
the [OpenRouter model catalogue](https://openrouter.ai/models), then change:

```text
LLM_MODEL_OPENROUTER=<provider>/<model-id>
```

If the chat model should use a different OpenRouter embedding model, also set:

```text
EMBEDDING_MODEL_OPENROUTER=<provider>/<embedding-model-id>
```

Use a model that supports OpenRouter's OpenAI-compatible chat-completions API.
Codebook generation also needs a valid embeddings model. Recreate the API
container after editing `Backend/.env`.

## Add Several Selectable OpenRouter Profiles

Changing `LLM_MODEL_OPENROUTER` replaces the one OpenRouter choice. To show
several models as separate choices on the Home page:

1. Add uniquely named chat and embedding settings in
   `Backend/app/config.py`, for example `LLM_MODEL_OPENROUTER_RESEARCH` and
   `EMBEDDING_MODEL_OPENROUTER_RESEARCH`.
2. Add a `ProviderSpec` in `Backend/app/llm/providers.py` with a unique `id` and
   label. It can reuse `LLM_API_KEY_OPENROUTER` and
   `LLM_BASE_URL_OPENROUTER`, while its `model_attr` and
   `embedding_model_attr` point to the new settings.
3. Document the corresponding values in `Backend/.env.example`.
4. Add provider-registry, chat-routing, embedding-routing, and settings-API
   tests based on the existing OpenRouter tests.
5. Recreate the API container. The new profile then appears automatically in
   the Home-page provider selector.

Do not accept a model ID directly from an untrusted browser request. Keeping the
profiles in the server-side registry limits selection to configurations that an
administrator has reviewed and tested.
