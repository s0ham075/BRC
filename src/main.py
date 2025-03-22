import mmap
import multiprocessing
import os
from math import ceil

CPU_COUNT = os.cpu_count()
MMAP_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")

# def to_int(x: bytes) -> int:
#     if x[0] == 45:  # ASCII for "-"
#         sign = -1
#         idx = 1
#     else:
#         sign = 1
#         idx = 0
#     # Check the position of the decimal point
#     if x[idx + 1] == 46:  # ASCII for "."
#         # -#.# or #.#
#         # 528 == ord("0") * 11
#         result = sign * ((x[idx] * 10 + x[idx + 2]) - 528)
#     else:
#         # -##.# or ##.#
#         # 5328 == ord("0") * 111
#         result = sign * ((x[idx] * 100 + x[idx + 1] * 10 + x[idx + 3]) - 5328)

#     return result

def reduce(results):
    final = {}
    for result in results:
        for city, item in result.items():
            if city in final:
                city_result = final[city]
                city_result[0] += item[0]
                city_result[1] += item[1]
                city_result[2] = min(city_result[2], item[2])
                city_result[3] = max(city_result[3], item[3])
            else:
                final[city] = item
    return final

def process_line(line, result):
    if not line or line == b'\n':
        return
    idx = line.find(b";")
    city = line[:idx]
    idli_int = float(line[idx + 1 : -1])
    if city in result:
        item = result[city]
        item[0] += 1
        item[1] += idli_int
        item[2] = min(item[2], idli_int)
        item[3] = max(item[3], idli_int)
    else:
        result[city] = [1, idli_int, idli_int,idli_int]

def align_offset(offset, page_size):
    return (offset // page_size) * page_size


def process_chunk(input_file_name, start_byte, end_byte):
    offset = align_offset(start_byte, MMAP_PAGE_SIZE)
    result = {}

    with open(input_file_name, "rb") as file:
        length = end_byte - offset

        with mmap.mmap(
            file.fileno(), length, access=mmap.ACCESS_READ, offset=offset
        ) as mmapped_file:
            mmapped_file.seek(start_byte - offset)
            # for line in iter(mmapped_file.readline, b"\n"):
            #     process_line(line, result)
            line = mmapped_file.readline()
            while line:  # Continue until we get an empty line
                process_line(line, result)
                line = mmapped_file.readline()
    return result


def read_file_in_chunks(input_file_name,output_file_name):
    file_size_bytes = os.path.getsize(input_file_name)
    base_chunk_size = file_size_bytes // CPU_COUNT
    chunks = []

    with open(input_file_name, "r+b") as file:
        with mmap.mmap(
            file.fileno(), length=0, access=mmap.ACCESS_READ
        ) as mmapped_file:
            start_byte = 0
            for _ in range(CPU_COUNT):
                end_byte = min(start_byte + base_chunk_size, file_size_bytes)
                end_byte = mmapped_file.find(b"\n", end_byte)
                end_byte = end_byte + 1 if end_byte != -1 else file_size_bytes
                chunks.append((input_file_name, start_byte, end_byte))
                start_byte = end_byte

    with multiprocessing.Pool(processes=CPU_COUNT) as p:
        results = p.starmap(process_chunk, chunks)

    final = reduce(results)

    sorted_cities = sorted(final.keys(), key=lambda x: x.decode())
    lines = []
    for city in sorted_cities:
        data = final[city]
        x = ceil((data[1] / data[0]) * 10) / 10
        lines.append(f"{city.decode()}={data[2]:.1f}/{x}/{data[3]:.1f}\n") 
    
    data = "".join(lines).encode("utf-8")
    # with open(output_file_name, "w") as f:
    #    f.writelines(lines)
        # f.write("h")
        # with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE) as mapped_file:
        #     mapped_file.write(data)
    with open(output_file_name, "wb+") as f:
    # Write a dummy byte so that the file isn’t empty
        f.write(b"h")
    # Resize the file to the required size (dummy + data length)
        f.truncate(1 + len(data))
    
    # Create an mmap with the new file size
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE) as mapped_file:
        # Overwrite from the beginning; this replaces the dummy
            mapped_file.seek(0)
            mapped_file.write(data)
    
    # Truncate the file to the actual data length (removing any extra dummy if present)
        f.truncate(len(data))

           
def main(input_file_name = "testcase.txt", output_file_name = "output.txt"):
    read_file_in_chunks(input_file_name,output_file_name)
    
if __name__ == "__main__":
    main()
