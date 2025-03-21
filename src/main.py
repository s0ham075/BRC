from math import ceil

def process_input_file(filepath: str):
    stats = {}

    with open(filepath, "rb") as f:
        for row in f:
            city, idli_str = row.strip().split(b";")
            idli = float(idli_str)
            if city in stats:
                stats_city = stats[city]
                stats_city[0] = min(stats[city][0], idli)
                stats_city[1] = max(stats[city][1], idli)
                stats_city[2] += idli
                stats_city[3] += 1
            else:
                stats[city] = [idli, idli, idli, 1]
                
    return stats


def output_stats(stats: dict, filepath: str):
    with open(filepath, "w") as f:
        for city, data in sorted(stats.items()):
            f.write(f"{city.decode()}={data[0]}/{ceil((data[2]/data[3]) * 10) / 10}/{data[1]}\n")

    # with open(filepath, "w") as f:
    #     lines = []
    #     for city, data in sorted(stats.items()):
    #         lines.append(f"{city.decode()}={data[0]}/{ceil((data[2]/data[3]) * 10) / 10}/{data[1]}\n")  
    #     f.writelines(lines)  

def main(input_file_name = "testcase.txt", output_file_name = "output.txt"):
    output_stats(process_input_file(input_file_name), output_file_name)

if __name__ == "__main__":
    main()
