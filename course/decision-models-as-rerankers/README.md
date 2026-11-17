# Decision Models as Rerankers

Notebooks for Adam Hevenor's guest session in AI-Powered Search (November 18, 2026).

- **Slides:** [hevmind.com/decks/decision-models-as-rerankers](https://hevmind.com/decks/decision-models-as-rerankers/)
- **[colab_free_tier.ipynb](colab_free_tier.ipynb)** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/treygrainger/ai-powered-search/blob/main/course/decision-models-as-rerankers/colab_free_tier.ipynb):
  runs an open decision model (Perplexity's `pplx-decider-flash-v1.3`, 4B, Apache-2.0) on a free Colab T4 GPU.
  No API keys. About 15 minutes end to end.
- **[hosted_walkthrough.ipynb](hosted_walkthrough.ipynb)** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/treygrainger/ai-powered-search/blob/main/course/decision-models-as-rerankers/hosted_walkthrough.ipynb):
  the same steps against hosted decision models (Jev, Perplexity, Cloudflare Clef, OpenAI), using whichever API keys
  you have. Cells for a provider without a key skip cleanly.

Both follow the slides: each request shape (pairwise, listwise batch, listwise choice) by hand first, then the same
three with the [`hev-rerank`](https://github.com/hev/reranker) library, then nDCG@10 before and after on a SciFact
subset, and a custom prompt.

These are copies of the notebooks in [hev/reranker](https://github.com/hev/reranker/tree/main/notebooks), where
they're maintained. Benchmark results across every decision model and purpose-built reranker:
[hevmind.com/research/rerankers](https://hevmind.com/research/rerankers/).
