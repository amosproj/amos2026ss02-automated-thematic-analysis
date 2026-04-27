# Model Selection for Automated Thematic Analysis

## Hardware Availability at FAU Cluster

The Friedrich-Alexander-Universität (FAU) provides access to various GPU clusters suitable for different computational needs:

| Cluster Name | GPU Model | VRAM pro GPU | GPU Count | Use Case |
| --- | --- | --- | --- | --- |
| **Helma** (Flagship) | NVIDIA H100 | 94 GB (HBM2e) | 384 | High-end parallel execution of large models (Qwen 72B, Llama 70B) |
| **Alex** (Workhorse) | NVIDIA A100 | 40 GB or 80 GB | 304 | Standard cluster for heavy AI tasks; 80 GB VRAM sufficient for Gemma 4 (31B) or quantized Llama 3 70B |
| **Alex** (Workhorse) | NVIDIA A40 | 48 GB (DDR6) | 352 | Slightly slower memory than A100; excellent for standard inference and JSON generation |
| **TinyGPU** (Test/Dev) | Various (Tesla V100, RTX 3080, RTX 2080 Ti) | 10-32 GB | ~35 | Ideal for testing, prototyping, text embeddings for BERTopic |

---

## Candidate Models

### 1. Qwen Class (e.g., qwen3.5:35b-a3b)

Alibaba's Qwen family represents the state-of-the-art among non-American model providers.

#### ✅ Strengths (Qwen)

1. **Multilingual Champion**: Approaches native German speaker level. Understands sarcasm, dialectal variations, and subtle nuances in interviews better than most open models.

2. **Excellent Format Compliance**: Rigorously follows systemic prompts. When JSON is requested, Qwen reliably delivers valid JSON without preamble (prevents code pipeline failures).

3. **MoE Speed**: Only billions of active parameters during inference (~3B), enabling rapid transcript processing through pipelines with significant computational savings.

4. **Giant Context Window**: Extremely large context windows (often up to 128k tokens). Can process a three-hour interview plus a five-page codebook in one prompt without information loss.

5. **Strong Logic Deduction**: Excellent at distinguishing between very similar categories in code assignments (Step 3), with razor-sharp precision.

#### ❌ Critical Weaknesses (Qwen)

1. **Over-Alignment (Censorship)**: Strict safety filters inherited from Chinese origins. Refuses to analyze transcripts containing sensitive, toxic, political, or highly emotional content, often producing empty output.

2. **Cultural Bias**: Training data reflects Asian perspectives; can produce slightly skewed interpretations of Western/European concepts or metaphors.

3. **VRAM Misconception**: Even with only ~3B active parameters, all ~35B parameters must reside in GPU VRAM. Requires large GPUs (A100 equivalent) despite theoretical efficiency.

4. **Quantization Sensitivity**: MoE models like Qwen lose precision faster than dense models when compressed (e.g., 4-bit quantization), leading to citation hallucinations.

5. **"Needle-in-Haystack" Weakness**: May overlook critical quotes hidden in the middle of extremely long transcripts near the context limit.

---

### 2. Mixtral 8x7B (Mistral's European Alternative)

#### ✅ Strengths (Mixtral)

1. **European Training Data**: No inherent Asian or purely American bias. Understands European contexts, legal frameworks, and social norms more appropriately.

2. **Perfect Tool Ecosystem**: Frameworks like vLLM and Instructor are optimally tuned for Mixtral. Performs excellently out-of-the-box on university clusters.

3. **High Censorship Robustness**: Significantly less filtered than Qwen. Analyzes interviews about crime, drugs, extremist views, or hardship completely neutrally without blocking.

4. **Top Chain-of-Thought**: When prompted to explain code assignment steps ("Why did you choose Code X?"), delivers extremely transparent and comprehensible reasoning.

5. **Strong Entity Recognition**: Excels at cleanly extracting concrete actors, locations, and specialized terminology from text into JSON.

#### ❌ Critical Weaknesses (Mixtral)

1. **Multilingual Tax Effect**: German is good but imperfect. Tends toward slightly awkward, translation-like sentence structures when summarizing "main points" (anglicisms).

2. **Chattiness**: Unpleasantly inclined to provide explanations. Without strict API enforcement, writes preambles like "Here is the desired analysis:" before JSON brackets.

3. **Smaller Effective Context**: Despite technically supporting 32k context, concentration degradation occurs above 16,000 tokens (~12,000 words). Very long interviews may require chunking.

4. **Direct Citation Weakness**: More frequently than Qwen, grammatically "corrects" quotes rather than copying them verbatim—prohibited in qualitative analysis.

5. **Aging Architecture**: Increasingly outdated in the rapid AI market. Struggles with very fine, almost philosophical distinctions between categories.

---

### 3. Gemma 4 31B Dense

#### ✅ Strengths

1. **Massive Context Window (256k Tokens)**: Absolute game-changer. Load multiple complete interview transcripts plus extensive codebook in one prompt without comprehension loss.

2. **Improved Multilinguality**: Unlike Gemma 2's severe "multilingual tax," the Gemma 3/4 family trained on 140+ languages. Excellent German with nuanced and sarcasm-aware comprehension.

3. **True Apache 2.0 License**: Completely free from commercial or restrictive research licenses. Ensures absolute legal security and sovereignty for university projects and future publications.

4. **Strong Reasoning**: Google prioritized logical deduction. Model provides razor-sharp categorization decisions between Code A/B with justifications rivaling the largest models.

5. **Multimodal Foundation**: While primarily processing text, trained to understand audio and images. Future capability to directly analyze interview audio files for emphasis and hesitation data.

#### ❌ Critical Weaknesses

1. **Resource Hunger (Dense Architecture)**: Unlike MoE models, all 31 billion parameters compute for each generated token. Noticeably slower and more computationally intensive than comparable MoE counterparts.

2. **KV-Cache Trap**: The massive 256k context window is impressive, but full utilization explodes VRAM requirements. Very long interviews can trigger "Out of Memory" errors despite sufficient total GPU memory.

3. **Limited Best Practices (Too New)**: Extremely recent market arrival. Frameworks like Instructor or vLLM may contain bugs, and prompt engineering requires pioneering work lacking established academic papers on Gemma 4 JSON stability.

4. **Google-Typical Over-Alignment**: Like Qwen, Google models are "security-conscious." Interviews containing violence, illegal behavior, extreme politics, or explicit medical details trigger safety mechanisms that block analysis.

5. **Context Degradation with Length**: While accepting 256k tokens, precision decreases as text increases ("needle-in-haystack" performance). Codebook placement at the beginning with extremely long interviews reduces sub-theme assignment precision toward text ends.

---

## Summary Comparison

| Criterion | Qwen | Mixtral | Gemma 4 |
| --- | --- | --- | --- |
| **German Quality** | Excellent | Good | Excellent |
| **Speed (MoE)** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| **Context Window** | 128k | 32k | 256k |
| **Censorship Risk** | High | Low | Medium |
| **Citation Accuracy** | High | Medium | High |
| **Reasoning** | Strong | Good | Strong |
| **Production Readiness** | High | High | Medium |

---

## Recommendations

- **Best Overall for German Interviews**: Qwen (if content is relatively safe)
- **Best for Sensitive Content**: Mixtral (highest censorship robustness)
- **Best for Long Interviews + Codebooks**: Gemma 4 (massive context; monitor VRAM usage)
- **Best for Prototyping**: Start with Mixtral on **TinyGPU** cluster, then scale to **Alex** (A100)
