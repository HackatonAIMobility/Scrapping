# Scrapping - Data Feeder for LLM Models

## Introduction

This repository contains the core components responsible for collecting data from various internet and social media sources. The primary goal is to serve as a robust feeder for Large Language Models (LLMs), providing them with timely and relevant information for analysis, sentiment measurement, and insight generation, similar to the principles outlined in the MAI (Mexican Analytics & Insights) project.

This module is a crucial part of a larger system that aims to understand and process public sentiment and information, particularly for domains like railway systems. By continuously gathering data, it enables the LLM to perform tasks such as identifying user "pains" and "gains" and contributing to a comprehensive "Customer Happiness Index."

## Project Components

*   `scrapper_main.py`: This script is the primary entry point for initiating data collection from various configured sources (e.g., social media APIs, websites). It orchestrates the scraping process, handling different data connectors and ensuring efficient data retrieval.
*   `api_server.py`: This file likely implements an API endpoint or service that allows other modules (e.g., the LLM processing unit) to request and receive the collected raw or pre-processed data. It acts as an interface between the data collection layer and the LLM consumers.

## Setup Instructions

To get this scrapping module up and running, follow these steps:

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd Scrapping
    ```

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install Dependencies:**
    This project relies on several Python libraries for web scraping, API interaction, and data handling. While a `requirements.txt` file is not currently provided, you will typically need libraries such as `requests`, `BeautifulSoup4`, `tweepy` (for Twitter), `selenium`, or specific API client libraries depending on the social media platforms and websites you intend to scrape.

    You can install them individually or create a `requirements.txt` file (e.g., `pip freeze > requirements.txt`) and install them in bulk:
    ```bash
    pip install requests beautifulsoup4 # Example libraries
    # OR if you create a requirements.txt:
    # pip install -r requirements.txt
    ```
    *Note: Please consult the specific scraping needs and API documentation for each data source to determine the exact dependencies.*

4.  **Configuration and Environment Variables:**
    Some scrapers, especially social media APIs, require API keys, tokens, or other credentials. These should be stored securely, ideally as environment variables. Create a `.env` file in the root of the `Scrapping` directory (or set them directly in your environment) for sensitive information.

    Example `.env` file:
    ```
    TWITTER_API_KEY=your_twitter_api_key
    TWITTER_API_SECRET=your_twitter_api_secret
    # Add other necessary API keys or configuration here
    ```
    You might need a library like `python-dotenv` to load these variables: `pip install python-dotenv`.

## How to Run

After setting up the environment and installing dependencies:

1.  **Start the Scrapper:**
    To begin collecting data, run the main scrapper script:
    ```bash
    python scrapper_main.py
    ```
    This script will execute the configured scraping tasks and store the collected data (e.g., in a database, local files, or directly pass it to the API server if integrated).

2.  **Start the API Server:**
    If you need to expose the collected data via an API for the LLM or other services, run the API server:
    ```bash
    python api_server.py
    ```
    The API server will typically listen on a specified port (e.g., `http://localhost:8000`) and provide endpoints to access the data. Refer to `api_server.py` for specific endpoint details and usage.

## Contribution

This module is designed to be extensible. Contributions to add new data sources, improve scraping efficiency, or enhance the API are welcome.
