# AI-Powered Search

Code examples for the book [_AI-Powered Search_](https://aipowerersearch.com) by [Trey Grainger](https://www.linkedin.com/in/treygrainger/), [Doug Turnbull](https://www.linkedin.com/in/softwaredoug/), and [Max Irwin](https://www.linkedin.com/in/maxirwin/). Published by [Manning Publications](https://www.manning.com).

## Book Overview
[_AI-Powered Search_](https://aipowerersearch.com) teaches you the latest machine learning techniques to build search engines that continuously learn from your users and your content to drive more domain-aware and intelligent search.

Search engine technology is rapidly evolving, with Artificial Intelligence (AI) driving much of that innovation. Crowdsourced relevance and the integration of large language models (LLMs) like GPT and other foundation models are massively accelerating the capabilities and expectations of search technology.

AI-Powered Search will teach you modern, data-science-driven search techniques like: 
- Semantic search using dense vector embeddings from foundation models
- Retrieval Augmented Generation
- Question answering and summarization combining search and LLMs
- Fine-tuning transformer-based LLMs
- Personalized search based on  user signals and vector embeddings
- Collecting user behavioral signals and building signals boosting models
- Semantic knowledge graphs for domain-specific learning
- Implementing machine-learned ranking models (learning to rank)
- Building click models to automate machine-learned ranking
- Generative search, hybrid search, and the search frontier

Today’s search engines are expected to be smart, understanding the nuances of natural language queries, as well as each user’s preferences and context. This book empowers you to build search engines that take advantage of user interactions and the hidden semantic relationships in your content to automatically deliver better, more relevant search experiences.

## How to run
For simplicity of setup, all code is shipped in Jupyter Notebooks and packaged in Docker containers. This means that installing Docker and then pulling (or building) and running the book's Docker containers is the only necessary setup.

Appendix A of [the book](https://aipoweredsearch.com) provides full step-by-step instructions for running the code examples. To get up and running quickly, however, pull the book's source code and run:
```
cd docker
docker-compose up
```

Once the containers are built and running (may take a while, especially on the first build), visit:
`http://localhost:8888` to pull up all the Jupyter notebooks and run the code examples live.

## Supported Technologies
AI-Powered Search teaches many modern search techniques leveraging machine learning approaches. While we utilize specific technologies to demonstrate concepts, most techniques are applicable to many modern search engines and vector databases.

Throughout the book, all code examples are in Python, with PySpark (the Python interface to Apache Spark) being utilized heavily for data processing tasks. The default search engine leveraged by the book's examples is Apache Solr, but most examples are abstracted away from the particular search engine, and swappable implementation will be soon available for the most popular search engines and vector databases.

[ *Note*: if you work for a search engine / vector database company or project and want to work with us on getting your engine supported, please reach out to trey@searchkernel.com ]

## Questions and help
Your purchase of _AI-Powered Search_ includes online access to Manning's [LiveBook forum](https://livebook.manning.com/forum?product=graingert). This allows you to provide comments and ask questions about any parts of the book. Additionally, feel free to submit pull requests, Github issues, or comments on the project's official Github repo at https://github.com/treygrainger/ai-powered-search.

## License
All code in this repository is open source under the [Apache License, Version 2.0 (ASL 2.0)](https://www.apache.org/licenses/LICENSE-2.0), unless otherwise specified.

Note that when executing the code, it may pull additional dependencies that follow alternate licenses, so please be sure to inspect those licenses before using them in your projects to ensure they are suitable. The code may also pull in datasets subject to various licenses, some of which may be derived from AI models and some of which may be derived from web crawls of data subject to fair use under the copyright laws in the country of publication (the USA). Any such datasets are published "as-is", for the sole purpose of demonstrating the concepts in the book, and these datasets and their associated licenses may be subject to change over time.

## Grab a copy of the book
If you don't yet have a copy, please support the authors and the publisher by purchasing a copy of [_AI-Powered Search_](http://aipowerersearch.com). It will walk you step by step through the concepts and techniques shown in the code examples in this repository, providing needed context and insights to help you better understand the techniques.