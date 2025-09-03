from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_community.vectorstores import FAISS
from langchain.chains import RetrievalQA
from rag.documents import load_documents


def get_qa_chain():
    docs = load_documents()
    embeddings = OpenAIEmbeddings()
    vectorstore = FAISS.from_documents(docs, embeddings)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 2})
    return RetrievalQA.from_chain_type(
        llm=ChatOpenAI(model="gpt-4"),
        retriever=retriever,
        return_source_documents=True,
    )


qa_chain = get_qa_chain()
