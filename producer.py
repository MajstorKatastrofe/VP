import requests, json
from kafka import KafkaProducer

STREAM_URL = "http://localhost:5000/stream/type1"
TOPIC = "udemy.courses.raw"

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    resp = requests.get(STREAM_URL, stream=True)
    if resp.status_code != 200:
        print("Failed to connect to Docker stream. Status code:", resp.status_code)
        return

    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        s = line.strip()

        # kraj streama (plain "close")
        if s.lower() == "close":
            producer.send(TOPIC, {"_control": "close"})
            break

        # SSE payload linije počinju sa "data:"
        if s.startswith("data:"):
            payload = s[5:].strip()  # skini "data:" prefiks
            if not payload:
                continue
            try:
                obj = json.loads(payload)   # OVDE je već dict, nema drugog json.loads!
            except json.JSONDecodeError:
                print("Ignoring non-JSON payload:", payload[:120])
                continue

            producer.send(TOPIC, obj)
        else:
            # korisno videti šta se ignoriše (event:, id:, prazno...)
            # print("Ignoring non-data line:", s)
            pass

    producer.flush()
    producer.close()
    print("Producer closed.")

if __name__ == "__main__":
    main()
