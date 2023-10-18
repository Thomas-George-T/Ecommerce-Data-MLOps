import requests
import zipfile

def ingest_data():
  
  file_url = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"

  # Send an HTTP GET request to the URL
  response = requests.get(file_url)

  # Check if the request was successful (status code 200)
  if response.status_code == 200:
      # Save file to data
      with open("data/data.zip", "wb") as file:
          file.write(response.content)
      print("File downloaded successfully.")
  else:
      print(f"Failed to download the file. Status code: {response.status_code}")


def unzip_file():
    zip_filename ='data/data.zip'
    extract_to = 'data/'
    try:
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"File {zip_filename} successfully unzipped to {extract_to}")
    except Exception as e:
        print(f"Failed to unzip {zip_filename}: {e}")


if __name__ == "__main__":
    ingest_data()
    unzip_file()