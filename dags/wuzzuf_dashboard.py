from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

def extract_jobs(**context):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)
    all_jobs = []

    try:
        for page in range(10):
            url = (
                f'https://wuzzuf.net/search/jobs/?q=data'
                f'&filters[country][0]=Egypt'
                f'&filters[post_date][0]=within_24_hours'
                f'&start={page}'
                f'&a=navbg'
            )
            driver.get(url)
            time.sleep(3)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            job_cards = soup.find_all('div', {'class': 'css-ghe2tq'})

            if not job_cards:
                print(f"No more jobs at page {page}, stopping")
                break

            for card in job_cards:
                try:
                    title = card.find('h2').text.strip()
                    company = card.find('a', {'class': 'css-ipsyv7'}).text.strip()
                    location = card.find('span', {'class': 'css-16x61xq'}).text.strip()
                    link = "https://wuzzuf.net" + card.find('a', {'class': 'css-o171kl'})['href']
                    posted = card.find('div', {'class': 'css-eg55jf'}) or card.find('div', {'class': 'css-1jldrig'})
                    posted = posted.text.strip() if posted else 'N/A'

                    # ============ Job Type ============
                    job_types = []
                    for tag in card.find_all('a', {'class': 'css-a85cz4'}):
                        span = tag.find('span')
                        if span:
                            job_types.append(span.text.strip())
                    for tag in card.find_all('a', {'class': 'css-uofntu'}):
                        span = tag.find('span')
                        if span:
                            job_types.append(span.text.strip())
                    job_type = ', '.join(job_types) if job_types else 'N/A'

                    # ============ Experience ============
                    experience = 'N/A'
                    exp_div = card.find('div', {'class': 'css-1rhj4yg'})
                    if exp_div:
                        for link_tag in exp_div.find_all('a', {'class': 'css-o171kl'}):
                            text = link_tag.text.strip()
                            if any(k in text for k in ['Level', 'Experienced', 'Manager', 'Student']):
                                experience = text
                                break
                        for span in exp_div.find_all('span'):
                            if 'Yrs' in span.text:
                                experience = experience + ' | ' + span.text.strip()
                                break

                    all_jobs.append({
                        'title': title,
                        'company': company,
                        'location': location,
                        'link': link,
                        'posted': posted,
                        'experience': experience,
                        'job_type': job_type
                    })
                except:
                    continue

            print(f"Page {page}: extracted {len(job_cards)} jobs, total: {len(all_jobs)}")

    finally:
        driver.quit()

    print(f"Total extracted: {len(all_jobs)} jobs")
    context['ti'].xcom_push(key='raw_jobs', value=all_jobs)


# ============ TRANSFORM ============
def transform_jobs(**context):
    raw_jobs = context['ti'].xcom_pull(key='raw_jobs', task_ids='extract_jobs')

    filtered = []
    for job in raw_jobs:
        if 'data' in job['title'].lower():
            filtered.append(job)

    print(f"Filtered {len(filtered)} data jobs from {len(raw_jobs)} total")
    context['ti'].xcom_push(key='filtered_jobs', value=filtered)


# ============ LOAD ============
def load_jobs(**context):
    jobs = context['ti'].xcom_pull(key='filtered_jobs', task_ids='transform_jobs')

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wuzzuf_jobs_dashboard (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            company VARCHAR(255),
            location VARCHAR(255),
            link TEXT UNIQUE,
            posted VARCHAR(100),
            experience VARCHAR(100),
            job_type VARCHAR(100),
            scraped_at TIMESTAMP DEFAULT NOW()
        );
    """)

    new_jobs = []
    for job in jobs:
        try:
            cur.execute("""
                INSERT INTO wuzzuf_jobs_dashboard (title, company, location, link, posted, experience, job_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING
                RETURNING id;
            """, (
                job['title'], job['company'], job['location'],
                job['link'], job['posted'], job['experience'], job['job_type']
            ))

            if cur.fetchone():
                new_jobs.append(job)
        except Exception as e:
            print(f"Error inserting job: {e}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {len(new_jobs)} new jobs")
    context['ti'].xcom_push(key='new_jobs', value=new_jobs)


# ============ NOTIFY ============
def notify_new_jobs(**context):
    new_jobs = context['ti'].xcom_pull(key='new_jobs', task_ids='load_jobs')

    if not new_jobs:
        print("No new jobs, skipping email")
        return

    jobs_html = "".join([f"""
        <tr>
            <td>{job['title']}</td>
            <td>{job['company']}</td>
            <td>{job['location']}</td>
            <td>{job['job_type']}</td>
            <td>{job['experience']}</td>
            <td>{job['posted']}</td>
            <td><a href="{job['link']}">Apply</a></td>
        </tr>
    """ for job in new_jobs])

    html = f"""
    <h2>📊 {len(new_jobs)} New Data Jobs in Egypt!</h2>
    <table border="1" cellpadding="8" style="border-collapse:collapse;">
        <tr>
            <th>Title</th>
            <th>Company</th>
            <th>Location</th>
            <th>Type</th>
            <th>Experience</th>
            <th>Posted</th>
            <th>Link</th>
        </tr>
        {jobs_html}
    </table>
    """

    msg = MIMEMultipart()
    msg['Subject'] = f"📊 {len(new_jobs)} New Data Jobs in Egypt - Dashboard Update!"
    msg['From'] = "ahmed.mostafa.xq@gmail.com"
    msg['To'] = "ahmed.mostafa.xq@gmail.com"
    msg.attach(MIMEText(html, 'html'))

    with smtplib.SMTP('smtp.gmail.com', 587) as s:
        s.starttls()
        s.login('ahmed.mostafa.xq@gmail.com', 'yatlpfynlourbpub')
        s.send_message(msg)

    print(f"Email sent with {len(new_jobs)} new jobs!")


# ============ DAG ============
with DAG(
    dag_id='wuzzuf_dashboard',
    description="Collect all data jobs in Egypt for Power BI dashboard",
    start_date=datetime(2026, 5, 1),
    schedule="0 */6 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['wuzzuf', 'dashboard']
) as dag:

    extract = PythonOperator(
        task_id='extract_jobs',
        python_callable=extract_jobs
    )

    transform = PythonOperator(
        task_id='transform_jobs',
        python_callable=transform_jobs
    )

    load = PythonOperator(
        task_id='load_jobs',
        python_callable=load_jobs
    )

    notify = PythonOperator(
        task_id='notify_new_jobs',
        python_callable=notify_new_jobs
    )

    extract >> transform >> load >> notify