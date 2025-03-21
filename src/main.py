from math import ceil


def process_input_file(filepath: str):
    stats = {}

    with open(filepath, "r") as f:
        for row in f:
            city, idli_str = row.strip().split(";")
            idli = float(idli_str)
            if city in stats:
                stats[city][0] = min(stats[city][0], idli)
                stats[city][1] = max(stats[city][1], idli)
                stats[city][2] += idli
                stats[city][3] += 1
            else:
                stats[city] = [idli, idli, idli, 1]
                
    return stats


def output_stats(stats: dict, filepath: str):
    with open(filepath, "w") as f:
        for city, data in sorted(stats.items()):
            f.write(f"{city}={data[0]}/{ceil((data[2]/data[3]) * 10) / 10}/{data[1]}\n")

def main(input_file_name = "testcase.txt", output_file_name = "output.txt"):
    output_stats(process_input_file(input_file_name), output_file_name)

if __name__ == "__main__":
    main()
