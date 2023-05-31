from os import walk
from statistics import fmean

RESULTS_DIR_PATH = "./output/analytics/client_results"
QUERY1_RESULTS = "text1.txt.results"
QUERY2_RESULTS = "text2.txt.results"


def count_subdirectories(dir: str) -> int:
    return len(list(walk(dir))) - 1  # walk() also counts the root dir


def get_durations(filename: str):
    def get_time(line: str) -> float:
        return float(line.split(" ")[1])

    durations = {}

    with open(filename, "r") as file:
        line = file.readline()
        durations["reading"] = get_time(line)
        line = file.readline()
        durations["processing"] = get_time(line)

    return durations


def compute_average(durations: list) -> dict:
    reading_durations = []
    processing_durations = []
    for duration in durations:
        reading_durations.append(duration["reading"])
        processing_durations.append(duration["processing"])

    return {
        "reading": fmean(reading_durations),
        "processing": fmean(processing_durations),
    }


def main():
    query_durations = {"query1": [], "query2": []}

    for dirpath, _, _ in walk(RESULTS_DIR_PATH):
        # walk() also counts the root dir
        if dirpath == RESULTS_DIR_PATH:
            continue
        query1_filename = "/".join([dirpath, QUERY1_RESULTS])
        query_durations["query1"].append(get_durations(query1_filename))
        query2_filename = "/".join([dirpath, QUERY2_RESULTS])
        query_durations["query2"].append(get_durations(query2_filename))

    query_durations = {
        "query1": compute_average(query_durations["query1"]),
        "query2": compute_average(query_durations["query2"]),
    }

    print(query_durations)


if __name__ == "__main__":
    main()
