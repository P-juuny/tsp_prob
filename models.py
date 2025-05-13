from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey, Enum
from sqlalchemy.orm import relationship
from datetime import datetime
import enum
from database import Base

# Enum 정의
class UserType(enum.Enum):
    DRIVER = "DRIVER"
    ADMIN = "ADMIN"
    STORE = "STORE"

class DeliveryStatus(enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"

class DocumentType(enum.Enum):
    DRIVER_LICENSE = "DRIVER_LICENSE"
    VEHICLE_REGISTRATION = "VEHICLE_REGISTRATION"
    INSURANCE = "INSURANCE"
    OTHER = "OTHER"

class PointTransactionType(enum.Enum):
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    PAYMENT = "PAYMENT"
    REFUND = "REFUND"

# User 모델
class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    password = Column(String(255), nullable=False)
    name = Column(String(100), nullable=False)
    userType = Column(Enum(UserType), nullable=False)
    isApproved = Column(Boolean, default=False)
    createdAt = Column(DateTime, default=datetime.utcnow)
    subscriptionPlanId = Column(Integer, ForeignKey("subscription_plans.id"))
    pointBalance = Column(Integer, default=0)
    defaultPickupDate = Column(String(50))
    
    # Relationships
    subscriptionPlan = relationship("SubscriptionPlan", back_populates="users")
    storeInfo = relationship("StoreInfo", back_populates="user", uselist=False)
    driverInfo = relationship("DriverInfo", back_populates="user", uselist=False)
    documentsUploaded = relationship("DocumentUpload", back_populates="user")
    pointTransactions = relationship("PointTransaction", back_populates="user")
    ownedParcels = relationship("Parcel", foreign_keys="Parcel.ownerId", back_populates="owner")
    driverParcels = relationship("Parcel", foreign_keys="Parcel.driverId", back_populates="driver")

# DriverInfo 모델
class DriverInfo(Base):
    __tablename__ = "driver_info"
    
    id = Column(Integer, primary_key=True, index=True)
    userId = Column(Integer, ForeignKey("users.id"), unique=True)
    phoneNumber = Column(String(20))
    vehicleNumber = Column(String(20))
    regionCity = Column(String(50))  # 구역(시)
    regionDistrict = Column(String(50))  # 구역(구)
    
    # Relationship
    user = relationship("User", back_populates="driverInfo")

# StoreInfo 모델
class StoreInfo(Base):
    __tablename__ = "store_info"
    
    id = Column(Integer, primary_key=True, index=True)
    userId = Column(Integer, ForeignKey("users.id"), unique=True)
    address = Column(String(255))
    detailAddress = Column(String(255))
    expectedSize = Column(String(50))
    monthlyCount = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    pickupPreference = Column(String(100))
    
    # Relationship
    user = relationship("User", back_populates="storeInfo")

# Parcel 모델
class Parcel(Base):
    __tablename__ = "parcels"
    
    id = Column(Integer, primary_key=True, index=True)
    ownerId = Column(Integer, ForeignKey("users.id"))
    driverId = Column(Integer, ForeignKey("users.id"))
    productName = Column(String(255))
    size = Column(String(50))
    caution = Column(Boolean, default=False)
    recipientName = Column(String(100))
    recipientPhone = Column(String(20))
    recipientAddr = Column(String(255))
    detailAddress = Column(String(255))
    trackingCode = Column(String(50))
    status = Column(Enum(DeliveryStatus), default=DeliveryStatus.PENDING)
    pickupDate = Column(DateTime)
    completedAt = Column(DateTime)
    deliveryImageUrl = Column(String(500))
    createdAt = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    owner = relationship("User", foreign_keys=[ownerId], back_populates="ownedParcels")
    driver = relationship("User", foreign_keys=[driverId], back_populates="driverParcels")

# SubscriptionPlan 모델
class SubscriptionPlan(Base):
    __tablename__ = "subscription_plans"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    price = Column(Integer)
    grantedPoint = Column(Integer)
    createdAt = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    users = relationship("User", back_populates="subscriptionPlan")

# DocumentUpload 모델
class DocumentUpload(Base):
    __tablename__ = "document_uploads"
    
    id = Column(Integer, primary_key=True, index=True)
    userId = Column(Integer, ForeignKey("users.id"))
    type = Column(Enum(DocumentType))
    fileUrl = Column(String(500))
    uploadedAt = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    user = relationship("User", back_populates="documentsUploaded")

# PointTransaction 모델  
class PointTransaction(Base):
    __tablename__ = "point_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    userId = Column(Integer, ForeignKey("users.id"))
    amount = Column(Integer)
    type = Column(Enum(PointTransactionType))
    reason = Column(String(255))
    createdAt = Column(DateTime, default=datetime.utcnow)
    expiredAt = Column(DateTime)
    
    # Relationship
    user = relationship("User", back_populates="pointTransactions")