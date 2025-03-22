from math import ceil
import mmap
import multiprocessing
import threading
import queue

CHUNK_SIZE = 256 * 1024  # 256 KB

def reader_thread(filename, start_offset, end_offset, data_queue):
    """Thread function to read a chunk of data from file and put it in queue"""
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)
        
        # Find the start of a complete line
        if start_offset != 0:
            while start_offset < size and mm[start_offset] != ord('\n'):
                start_offset += 1
            start_offset += 1  
        
        # Find the end of a complete line
        end = end_offset
        while end < size and mm[end] != ord('\n'):
            end += 1
        if end < size:
            end += 1 
        
        # Read the data
        data = mm[start_offset:end]
        mm.close()
    
    # Put the data in the queue for the processor thread
    data_queue.put(data)

def processor_thread(data_queue, result_queue):
    """Thread function to process data from queue and put results in result queue"""
    cities = {}
    
    # Get data from queue
    data = data_queue.get()
    
    # Process the data
    lines = data.split(b'\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        semicolon_pos = line.find(b';')
        if semicolon_pos == -1:
            continue
        
        city = line[:semicolon_pos]
        idli_str = line[semicolon_pos+1:]
        try:
            idli = float(idli_str)
        except ValueError:
            continue
        
        if city in cities:
            entry = cities[city]
            entry[0] = min(entry[0], idli)
            entry[1] = max(entry[1], idli)
            entry[2] += idli
            entry[3] += 1
        else:
            cities[city] = [idli, idli, idli, 1]
    
    # Put results in the result queue
    result_queue.put(cities)

def process_chunk_with_threads(filename, start_offset, end_offset):
    """Process a chunk of the file using two threads"""
    # Create queues for communication between threads
    data_queue = queue.Queue()
    result_queue = queue.Queue()
    
    # Create and start the reader thread
    reader = threading.Thread(
        target=reader_thread, 
        args=(filename, start_offset, end_offset, data_queue)
    )
    
    # Create and start the processor thread
    processor = threading.Thread(
        target=processor_thread,
        args=(data_queue, result_queue)
    )
    
    # Start both threads
    reader.start()
    processor.start()
    
    # Wait for both threads to complete
    reader.join()
    processor.join()
    
    # Get and return the results
    return result_queue.get()

def merge_results(results):
    merged = {}
    for result in results:
        for city, stats in result.items():
            if city in merged:
                current = merged[city]
                current[0] = min(current[0], stats[0])
                current[1] = max(current[1], stats[1])
                current[2] += stats[2]
                current[3] += stats[3]
            else:
                merged[city] = list(stats)
    return merged

def write_large_data_to_file(filename, data, chunk_size=CHUNK_SIZE):
    """Write the given bytes object to a file in chunks of chunk_size."""
    with open(filename, "wb") as file:
        for i in range(0, len(data), chunk_size):
            file.write(data[i : i + chunk_size])

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    # Get file size
    with open(input_file_name, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)
        mm.close()
    
    # Divide the file into chunks for multiprocessing
    num_procs = multiprocessing.cpu_count()
    chunk_size = file_size // num_procs
    chunks = []
    for i in range(num_procs):
        start = i * chunk_size
        end = start + chunk_size if i < num_procs - 1 else file_size
        chunks.append((start, end))
    
    # Process each chunk in a separate process, using threads within each process
    with multiprocessing.Pool(num_procs) as pool:
        tasks = [(input_file_name, start, end) for start, end in chunks]
        results = pool.starmap(process_chunk_with_threads, tasks)
    
    # Merge the results from all processes
    merged = merge_results(results)
    
    # Format and write results
    sorted_cities = sorted(merged.keys(), key=lambda x: x.decode())
    lines = []
    for city in sorted_cities:
        data_vals = merged[city]
        x = ceil((data_vals[2] / data_vals[3]) * 10) / 10
        lines.append(
            f"{city.decode()}={data_vals[0]:.1f}/{x}/{data_vals[1]:.1f}\n".encode("utf-8")
        )
    
    data_bytes = b"".join(lines)
    with open(output_file_name, "wb") as f:
        f.write(data_bytes)

if __name__ == "__main__":
    main()
