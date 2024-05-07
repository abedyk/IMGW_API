import sqlite3
from requests import get
from datetime import date
import schedule
import time

# Inicjalizacja zbioru dla unikalnych wpisów
unique_entries = set()

print("Starting the script...")


def get_weather_for_location(location):
    print(f"Getting weather data for {location}...")
    response = get('https://danepubliczne.imgw.pl/api/data/synop')
    for row in response.json():
        if row['stacja'] == location:
            weather_data = {'godzina_pomiaru': row['godzina_pomiaru'], 'pressure': row['cisnienie'],
                            'temperature': row['temperatura']}
            print(f"Received weather data for {location}: {weather_data}")
            return weather_data
    return None


def add_weather(connection, created_at: date, location: str, weather):
    print(f"Adding weather data for {location} to the database...")
    added_at = time.strftime('%Y-%m-%d %H:%M:%S')  # Aktualna data i godzina
    cursor = connection.cursor()

    # Sprawdź, czy dane z danej stacji, daty i godziny pomiaru już istnieją w bazie
    cursor.execute("SELECT COUNT(*) FROM weather WHERE created_at = ? AND station = ? AND godzina_pomiaru = ?", (
        created_at,
        location,
        weather['godzina_pomiaru']
    ))
    existing_count = cursor.fetchone()[0]

    if existing_count > 0:
        print(
            f"Dane pogodowe dla {location} na godzinę {weather['godzina_pomiaru']} już istnieją w bazie. Pomijam dodanie.")
    else:
        # Jeśli dane nie istnieją, dodaj nowe dane do bazy
        cursor.execute(
            'INSERT INTO weather(created_at, added_at, godzina_pomiaru, station, temperature, pressure) VALUES(?, ?, ?, ?, ?, ?)',
            (
                created_at,
                added_at,
                weather['godzina_pomiaru'],
                location,
                weather['temperature'],
                weather['pressure'],
            ))
        connection.commit()
        print(f"Dodano dane pogodowe dla {location} na godzinę {weather['godzina_pomiaru']} do bazy danych.")

        # Dodaj wpis do zbioru unikalnych wpisów
        unique_entries.add((location, created_at, weather['godzina_pomiaru']))

    # Zapis do pliku logu
    with open("output4.log", "a", encoding="utf-8") as logfile:
        if existing_count > 0:
            logfile.write(
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Dane pogodowe dla {location} na godzinę {weather['godzina_pomiaru']} już istnieją w bazie.\n")
        else:
            logfile.write(
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Added weather data for {location} at {weather['godzina_pomiaru']}.\n")


def initialize(connection):
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather(
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
         added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
         godzina_pomiaru TEXT,
         station TEXT,
         temperature REAL,
         pressure REAL
    )''')
    connection.commit()


def get_initial_weather(connection):
    today = date.today()
    stations = ['Białystok', 'Gdańsk', 'Łódź', 'Kraków', 'Poznań', 'Suwałki', 'Szczecin', 'Warszawa', 'Wrocław']
    print("Getting initial weather data...")
    for station in stations:
        weather_data = get_weather_for_location(station)
        if weather_data:
            add_weather(connection, today, station, weather_data)
    print("Initial weather data retrieval completed.")


def job():
    today = date.today()
    stations = ['Białystok', 'Gdańsk', 'Łódź', 'Kraków', 'Poznań', 'Suwałki', 'Szczecin', 'Warszawa', 'Wrocław']
    print(f"Starting job at {time.strftime('%H:%M:%S')}")
    with open("output4.log", "a", encoding="utf-8") as logfile:
        logfile.write(f"Job started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    for station in stations:
        weather_data = get_weather_for_location(station)
        if weather_data:
            with sqlite3.connect('weather6.db') as connection:
                add_weather(connection, today, station, weather_data)
    print(f"Job completed at {time.strftime('%H:%M:%S')}")
    with open("output4.log", "a", encoding="utf8") as logfile:
        logfile.write(f"Job completed at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")


print("Initialization complete.")

if __name__ == "__main__":
    with sqlite3.connect('weather6.db') as connection:
        initialize(connection)

        # Pobierz pierwsze dane pogodowe po inicjalizacji
        get_initial_weather(connection)

        # Dodaj zadanie do harmonogramu, które będzie uruchamiane co 10 minut
        schedule.every(10).minutes.do(job)

        while True:
            schedule.run_pending()
            time.sleep(1)
