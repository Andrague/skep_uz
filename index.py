import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import sys

sys.stdout.reconfigure(encoding='utf-8')

Base = declarative_base()

class Employee(Base):
    __tablename__ = 'employees'
    id = Column(Integer, primary_key=True, autoincrement=True)
    degree = Column(String(20))
    first_name = Column(String(100))
    last_name = Column(String(100))
    publications = relationship('Publication', back_populates='author')

class Publication(Base):
    __tablename__ = 'publications'
    pub_id = Column(Integer, primary_key=True, autoincrement=True)
    author_id = Column(Integer, ForeignKey('employees.id'))
    pub_name = Column(Text)
    author = relationship('Employee', back_populates='publications')

DATABASE_URL = 'mysql+mysqlconnector://root:@localhost/dvlp'
engine = create_engine(DATABASE_URL, echo=True)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def scrape_data():
    base_url = 'https://pers.uz.zgora.pl'
    main_url = f'{base_url}/publikacje-instytuty/095028'
    response = requests.get(main_url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table')
    if not table:
        print("Table not found. Please check the HTML structure.")
        return

    driver = webdriver.Chrome()
    driver.get(main_url)

    for row in table.find_all('tr')[1:]:  # Skip the header row
        cols = row.find_all('td')
        name_tag = cols[0].find('a')
        if not name_tag:
            continue
        full_name = name_tag.text.strip()

        # Extract degree and names
        name_parts = full_name.split()
        degree = ' '.join([part for part in name_parts if part.lower() in ["dr", "hab.", "inż.", "prof.", "zw.", "hab", "inz"]])
        name_surname = ' '.join([part for part in name_parts if part.lower() not in ["dr", "hab.", "inż.", "prof.", "zw.", "hab", "inz"]])
        
        try:
            first_name, last_name = name_surname.split(' ', 1)
        except ValueError:
            print(f"Skipping name with unexpected format: {full_name}")
            continue

        # Check if employee already exists
        existing_employee = session.query(Employee).filter_by(first_name=first_name, last_name=last_name).first()
        if existing_employee:
            print(f"Employee {first_name} {last_name} already exists in the database.")
            continue

        # Insert employee into the database
        employee = Employee(degree=degree, first_name=first_name, last_name=last_name)
        session.add(employee)
        session.commit()
        employee_id = employee.id

        # Print the employee info for debugging
        print(f"Added employee: {degree} {first_name} {last_name}, ID: {employee_id}")

        # Use Selenium to navigate to the employee's publication page
        try:
            employee_link = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, last_name))
            )
            employee_link.click()

            # Wait for the 'cały dorobek (zarejestrowany w systemie)' link to be present and click it
            publication_link = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, 'cały dorobek'))
            )
            publication_link.click()

            # Now we are on the next page; get its URL and analyze it for publications
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'table-striped')))
            next_page_url = driver.current_url

            # Scrape publications using the publication URL
            scrape_employee_publications(employee_id, next_page_url, first_name, last_name)

            # Go back to the main page to continue with the next employee
            driver.back()
            driver.back()
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))  # Wait for the table to be present again

        except Exception as e:
            print(f"Failed to process employee {full_name}: {e}")
            driver.back()
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))  # Wait for the table to be present again

    driver.quit()
    session.commit()

def scrape_employee_publications(employee_id, publication_url, first_name, last_name):
    response = requests.get(publication_url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table with class 'table table-striped'
    publication_table = soup.find('table', class_='table table-striped')
    if not publication_table:
        print("Publication table not found. Please check the HTML structure.")
        return

    #for row in publication_table.find_all('tr')[1:]:  # Skip the header row
    for row in publication_table.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) < 1:
            continue

        # Extract publication name
        pub_name_tag = cols[0].find('b')
        if not pub_name_tag:
            continue
        pub_name = pub_name_tag.text.strip()

        # Print the publication info for debugging
        print(f"Found publication: {pub_name}")

        # Insert into publications
        if pub_name:
            publication = Publication(author_id=employee_id, pub_name=pub_name)
            session.add(publication)
    
    session.commit()

if __name__ == '__main__':
    scrape_data()
    session.close()
