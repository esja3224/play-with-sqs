import csv

def main():
    with open('data.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        header_row = ["a", "b", "c", "d"]
        writer.writerow(header_row)

        for i in range(1000):
            row_data = [f"asdasdasdasdText {i+1}", float(i*0.12345), float((i+1)*0.23456), float((i+2)*0.34567)]
            writer.writerow(row_data)

if __name__ == "__main__":
    main()