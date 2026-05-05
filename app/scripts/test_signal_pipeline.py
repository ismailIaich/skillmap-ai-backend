from app.db.session import SessionLocal
from app.services.skill_signal.extractor import extract_skills_from_text
from app.services.skill_signal.aggregator import aggregate_skills


def test_pipeline(text: str):
    db = SessionLocal()

    try:
        print(f"\nINPUT: {text}")

        raw = extract_skills_from_text(db, text)
        clean = aggregate_skills(raw)

        print("\n=== RAW ===")
        for r in raw:
            print(r)

        print("\n=== CLEAN ===")
        for c in clean:
            print(c)

    finally:
        db.close()


if __name__ == "__main__":
    test_pipeline("I build machine learning and AI models with Python")
    test_pipeline("I build APIs with FastAPI and Python")
    test_pipeline("I manage teams and deploy apps with Docker")
    test_pipeline("I do nothing")
