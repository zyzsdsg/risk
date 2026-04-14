from app.db.session import engine
from app.db.models import Base


def init_db():
    Base.metadata.create_all(engine)
    print("created all tables")


if __name__ == "__main__":
    init_db()
