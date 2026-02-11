from db_loader import load_amazon_sales
from langchain_classic.schema import Document
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv
import time
import pandas as pd
import os

# Load environment variables

load_dotenv()


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_API_KEY =os.getenv("PINECONE_API_KEY")
INDEX_NAME = os.getenv("INDEX_NAME")

# Initialize embeddings

embeddings = OpenAIEmbeddings(api_key=OPENAI_API_KEY)

#Load sql data

df = load_amazon_sales()


# Clean column names: remove spaces, lowercase, add underscores

df.columns = (
    df.columns
    .str.strip()              # remove leading/trailing spaces
    .str.lower()              # convert to lowercase
    .str.replace(" ", "_")    # replace spaces with underscores
)


#Convert rows to documents (handle NaN)

documents = []

for _, row in df.iterrows():
    text = "\n".join([f"{col}: {row[col]}" for col in df.columns if pd.notna(row[col])])
    documents.append(Document(page_content=text))


# Initialize Pinecone

pc = Pinecone(api_key=PINECONE_API_KEY)

#Create index if not exists

if INDEX_NAME not in pc.list_indexes().names():
    pc.create_index(
        name= INDEX_NAME,
        dimension=1536,
        metric= "cosine",
        spec=ServerlessSpec(
            cloud = "aws",
            region="us-east-1"
        )
    )


    time.sleep(5) # wait for index to be ready


# Store vectors
vectorestore = PineconeVectorStore.from_documents(
    documents=documents,
    embedding=embeddings,
    index_name = INDEX_NAME
)


print("✅ Amazon SQL data indexed successfully")