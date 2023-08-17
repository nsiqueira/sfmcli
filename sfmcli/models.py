from __future__ import annotations

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from sfmcli.database import Base


class DataExtension(Base):
    __tablename__ = 'data_extension'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    origin_external_key = Column(String)
    origin_instance = Column(String)
    target_external_key = Column(String)
    target_instance = Column(String)

    pages: Mapped[list[DataExtensionPage]] = relationship(
        back_populates='data_extension',
        cascade='all,delete',
    )

    def __repr__(self):
        return f'<DataExtension(id={self.id}, name={self.name}, origin_external_key={self.origin_external_key}, origin_instance={self.origin_instance}, target_external_key={self.target_external_key}, target_instance={self.target_instance})>'  # noqa: E501


class DataExtensionPage(Base):
    __tablename__ = 'data_extension_page'

    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True)
    request_id = Column(String, unique=True)
    status = Column(String)
    data_extension_id = mapped_column(ForeignKey('data_extension.id'))
    has_sfmc_key = Column(Boolean)

    data_extension: Mapped[DataExtension] = relationship(
        back_populates='pages',
        lazy='subquery',
    )

    def __repr__(self):
        return f'<DataExtensionPage(id={self.id}, url={self.url}, request_id={self.request_id}, status={self.status}, data_extension_id={self.data_extension_id}, has_sfmc_key={self.has_sfmc_key})>'  # noqa: E501
