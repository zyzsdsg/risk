import logging
from app.config import settings


####Set up logging
log_level = logging.DEBUG if settings.debug else logging.INFO
logging.basicConfig(level=log_level)
