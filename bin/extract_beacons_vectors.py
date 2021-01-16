import inspect
import os
import sys

if __name__ == "__main__":
    current_dir = os.path.dirname(
        os.path.abspath(inspect.getfile(inspect.currentframe()))
    )
    parent_dir = os.path.dirname(current_dir)

    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    from src import main

    main.main()
