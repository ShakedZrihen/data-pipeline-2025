import sys
from handler import main

if __name__ == "__main__":
    event_file = sys.argv[1] if len(sys.argv) > 1 else "sample_event.json"
    main(event_file)
