from langchain.schema import Document


def load_documents():
    codenames = {
        "codename_fox": "Stealth operation in northern Italy in 1943",
        "codename_hawk": "Surveillance mission over the Pacific Ocean",
        "codename_lion": "Ground assault in desert terrain in 1991",
    }
    return [Document(page_content=f"{k}: {v}") for k, v in codenames.items()]
