import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    name = Column(String(255), nullable=False)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=False)  # Precisa aprovacao admin
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    approved_at = Column(DateTime, nullable=True)

    queries = relationship("UserQuery", back_populates="user")


class UserQuery(Base):
    __tablename__ = "user_queries"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    company_name = Column(String(255), nullable=True)
    query_text = Column(Text, nullable=False)
    response_text = Column(Text, nullable=True)
    feedback_rating = Column(Integer, nullable=True)  # 1-5
    feedback_comment = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    user = relationship("User", back_populates="queries")


class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, index=True)
    cod_cvm = Column(String(20), unique=True, index=True)
    cnpj = Column(String(20), nullable=True)
    nome = Column(String(255), nullable=False)
    nome_pregao = Column(String(255), nullable=True)
    setor = Column(String(255), nullable=True)
    situacao = Column(String(100), nullable=True)
    data_registro = Column(String(20), nullable=True)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)


class CompanyDocument(Base):
    __tablename__ = "company_documents"

    id = Column(Integer, primary_key=True, index=True)
    cod_cvm = Column(String(20), index=True, nullable=False)
    tipo = Column(String(50), nullable=False)  # ITR, DFP, FRE, IPE (fato relevante)
    descricao = Column(String(500), nullable=True)
    data_referencia = Column(String(20), nullable=True)
    data_entrega = Column(String(20), nullable=True)
    link_documento = Column(String(1000), nullable=True)
    versao = Column(String(10), nullable=True)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)


class FinancialData(Base):
    __tablename__ = "financial_data"

    id = Column(Integer, primary_key=True, index=True)
    cod_cvm = Column(String(20), index=True, nullable=False)
    tipo_documento = Column(String(10), nullable=False)  # ITR ou DFP
    data_referencia = Column(String(20), nullable=False)
    conta = Column(String(50), nullable=True)
    descricao_conta = Column(String(500), nullable=True)
    valor = Column(Float, nullable=True)
    escala = Column(String(20), nullable=True)
    moeda = Column(String(10), nullable=True)
    ordem_exercicio = Column(String(20), nullable=True)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)


class AnalysisPrompt(Base):
    __tablename__ = "analysis_prompts"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(255), unique=True, nullable=False)
    descricao = Column(String(500), nullable=True)
    prompt_template = Column(Text, nullable=False)
    categoria = Column(String(100), nullable=True)  # financeiro, estrategico, credito
    is_active = Column(Boolean, default=True)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)
