import requests
from bs4 import BeautifulSoup
import csv

url = "http://quotes.toscrape.com"

response = requests.get(url)
html_content = response.content

# Create a Beautiful Soup object to parse the HTML
soup = BeautifulSoup(html_content, 'html.parser')

# Extract specific information (quotes and authors in this case)
quotes = []
authors = []

for quote in soup.find_all('span', class_='text'):
    quotes.append(quote.text.strip())

for author in soup.find_all('small', class_='author'):
    authors.append(author.text.strip())

# Save the extracted information to a CSV file
output_file = 'quotes.csv'

with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Quote', 'Author']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for q, a in zip(quotes, authors):
        writer.writerow({'Quote': q, 'Author': a})

print(f"Quotes scraped successfully and saved to {output_file}.")
