"""
Servico de autenticacao com aprovacao de admin.
"""
from datetime import datetime, timedelta

import bcrypt
from jose import jwt, JWTError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.models import User


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def create_access_token(data: dict, expires_delta: timedelta = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_token(token: str) -> dict | None:
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError:
        return None


async def authenticate_user(db: AsyncSession, email: str, password: str) -> User | None:
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if user and verify_password(password, user.hashed_password):
        return user
    return None


async def create_user(db: AsyncSession, email: str, name: str, password: str, is_admin: bool = False) -> User:
    user = User(
        email=email,
        name=name,
        hashed_password=hash_password(password),
        is_active=is_admin,  # Admin ja nasce ativo
        is_admin=is_admin,
        approved_at=datetime.utcnow() if is_admin else None,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    result = await db.execute(select(User).where(User.email == email))
    return result.scalar_one_or_none()


async def approve_user(db: AsyncSession, user_id: int) -> User | None:
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user:
        user.is_active = True
        user.approved_at = datetime.utcnow()
        await db.commit()
        await db.refresh(user)
    return user


async def deactivate_user(db: AsyncSession, user_id: int) -> User | None:
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user and not user.is_admin:
        user.is_active = False
        await db.commit()
        await db.refresh(user)
    return user


async def get_all_users(db: AsyncSession):
    result = await db.execute(select(User).order_by(User.created_at.desc()))
    return result.scalars().all()


async def ensure_admin_exists(db: AsyncSession):
    """Cria usuario admin padrao se nao existir."""
    admin = await get_user_by_email(db, settings.ADMIN_EMAIL)
    if not admin:
        await create_user(db, settings.ADMIN_EMAIL, "Administrador", settings.ADMIN_PASSWORD, is_admin=True)
