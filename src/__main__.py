# __main__.py
from .cli import main
from .core.utils.logging import setup_logging

if __name__ == "__main__":
    setup_logging()
    main()