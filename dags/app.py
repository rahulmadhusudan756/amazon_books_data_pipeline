
#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from random import choice
from urllib.parse import quote_plus

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#1) fetch amazon data (extract) 2) clean data (transform)

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
]


def build_requests_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_amazon_data_books(num_books, ti, dag_run=None):
    search_term = "one piece manga books"
    if dag_run and getattr(dag_run, "conf", None):
        search_term = dag_run.conf.get("search_term", search_term)

    base_url = f"https://www.amazon.in/s?k={quote_plus(search_term)}"

    books = []
    seen_titles = set()
    session = build_requests_session()
    max_pages = 10

    page = 1

    while len(books) < num_books and page <= max_pages:
        url = f"{base_url}&page={page}"

        request_headers = dict(headers)
        request_headers["User-agent"] = choice(user_agents)

        try:
            response = session.get(url, headers=request_headers, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            book_containers = soup.find_all("div", {"class": "s-result-item"})

            page_books_before = len(books)
            for book in book_containers:
                title_link = book.find("h2")
                title = title_link.find("span") if title_link else None
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})
                
                if title and author and price and rating:
                    book_title = title.text.strip()
                    
                    # Check if title has been seen before
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                        })

            if len(books) == page_books_before:
                break

            page += 1
        except requests.RequestException as exc:
            raise AirflowException(f"Failed to fetch Amazon search results for '{search_term}' from {url}: {exc}") from exc

    if not books:
        raise AirflowException(f"No book data found for search term '{search_term}'")

    books = books[:num_books]

    df = pd.DataFrame(books)
    df.drop_duplicates(subset="Title", inplace=True)
    ti.xcom_push(key='book_data', value=df.to_dict('records'))

#3) create and store data in table on postgres (load)
    
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task

