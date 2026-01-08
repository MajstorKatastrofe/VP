from kafka import KafkaConsumer
import json, csv

TOPIC = "udemy.courses.raw"
CSV_PATH = "udemy_data.csv"

REQUIRED = [
    "id","title","url","is_paid","instructor_names","category","headline",
    "num_subscribers","rating","num_reviews","instructional_level","objectives","curriculum"
]

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["localhost:9092"],
        group_id="udemy-csv-consumer-1",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(REQUIRED)
        print("CSV header written →", CSV_PATH)

        count = 0
        for msg in consumer:
            data = msg.value

            # kraj iz našeg sistema
            if isinstance(data, dict) and data.get("_control") == "close":
                print("Close received. Total rows written:", count)
                break

            if not isinstance(data, dict):
                continue

            # preskoči poruke koje nemaju sva očekivana polja
            if not all(k in data for k in REQUIRED):
                # print("Skipping incomplete message:", data)
                continue

            row = [data.get(k, "") for k in REQUIRED]
            w.writerow(row)
            count += 1
            # ako želiš vidljiv progres:
            # if count % 500 == 0: print("rows:", count)

    consumer.close()
    print("Finished and saved →", CSV_PATH)

if __name__ == "__main__":
    main()
