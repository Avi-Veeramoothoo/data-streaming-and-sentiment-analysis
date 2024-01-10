import json
import socket
import time
import pandas as pd

def send_data_over_socket(file_path, host='127.0.0.1', port=9999, chunk_size=2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"Listening for connections on {host}:{port}")

    conn, addr = s.accept()
    print(f"Connection from {addr}")

    last_sent_index = 0
    try:
        with open(file_path, 'r') as file:
            # skip the data rows that were already sent
            for _ in range(last_sent_index):
                next(file)

            records = []
            for row in file:
                records.append(json.loads(row))
                if len(records)== chunk_size:
                    chunk = pd.DataFrame(records)
                    print(chunk)
                    for record in chunk.to_dict(orient='records'):
                        serialize_data = json.dump(record)
                        conn.send(serialize_data + b'\n') # \n acts as null terminator in C string
                        time.sleep(5)
                        last_sent_index += 1
                    records = []
    except (BrokenPipeError, ConnectionResetError):
        print("Client disconnected.")
    finally:
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    send_data_over_socket("datasets/yelp_academic_dataset_review.json")




