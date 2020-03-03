########################################################################################################################################################
#  SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC  #
########################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do SINASC para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e Microsoft SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do SINASC definido nas classes abaixo
Base = declarative_base()

# Tabela das Declarações de Nascimento (DN) (tabela principal do banco de dados)
class DNBR(Base):
    __tablename__ = 'dnbr'
    __table_args__ = {'schema': 'sinasc'}
    # Aqui se define as colunas para a tabela "dnbr" (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    NUMERODN = Column(String(8))                                                # Logical key
    UF_DN = Column(String(2))                                                   # Atributo
    ANO_DN = Column(Integer)                                                    # Atributo
    CODINST = Column(String(18))                                                # Atributo
    CODESTAB_ID = Column(String(7), ForeignKey('sinasc.codestab.ID'))           # Foreign key
    codestab = relationship('CODESTAB')
    CODMUNNASC_ID = Column(String(6), ForeignKey('sinasc.codmunnasc.ID'))       # Foreign key
    codmunnasc = relationship('CODMUNNASC')
    LOCNASC_ID = Column(String(2), ForeignKey('sinasc.locnasc.ID'))             # Foreign key
    locnasc = relationship('LOCNASC')
    IDADEMAE = Column(Float)                                                    # Atributo
    ESTCIVMAE_ID = Column(String(2), ForeignKey('sinasc.estcivmae.ID'))         # Foreign key
    estcivmae = relationship('ESTCIVMAE')
    ESCMAE_ID = Column(String(2), ForeignKey('sinasc.escmae.ID'))               # Foreign key
    escmae = relationship('ESCMAE')
    CODOCUPMAE_ID = Column(String(6), ForeignKey('sinasc.codocupmae.ID'))       # Foreign key
    codocupmae = relationship('CODOCUPMAE')
    QTDFILVIVO = Column(Float)                                                  # Atributo
    QTDFILMORT = Column(Float)                                                  # Atributo
    CODMUNRES_ID = Column(String(6), ForeignKey('sinasc.codmunres.ID'))         # Foreign key
    codmunres = relationship('CODMUNRES')
    GESTACAO_ID = Column(String(2), ForeignKey('sinasc.gestacao.ID'))           # Foreign key
    gestacao = relationship('GESTACAO')
    GRAVIDEZ_ID = Column(String(2), ForeignKey('sinasc.gravidez.ID'))           # Foreign key
    gravidez = relationship('GRAVIDEZ')
    PARTO_ID = Column(String(2), ForeignKey('sinasc.parto.ID'))                 # Foreign key
    parto = relationship('PARTO')
    CONSULTAS_ID = Column(String(2), ForeignKey('sinasc.consultas.ID'))         # Foreign key
    consultas = relationship('CONSULTAS')
    DTNASC = Column(Date)                                                       # Atributo
    HORANASC = Column(String(4))                                                # Atributo
    SEXO = Column(String(2))                                                    # Atributo
    APGAR1 = Column(Float)                                                      # Atributo
    APGAR5 = Column(Float)                                                      # Atributo
    RACACOR_ID = Column(String(2), ForeignKey('sinasc.racacor.ID'))             # Foreign key
    racacor = relationship('RACACOR')
    PESO = Column(Float)                                                        # Atributo
    IDANOMAL = Column(Numeric)                                                  # Atributo
    DTCADASTRO = Column(Date)                                                   # Atributo
    CODANOMAL_ID = Column(String(4), ForeignKey('sinasc.codanomal.ID'))         # Foreign key
    codanomal = relationship('CODANOMAL')
    DTRECEBIM = Column(Date)                                                    # Atributo
    DIFDATA = Column(Float)                                                     # Atributo
    NATURALMAE_ID = Column(String(3), ForeignKey('sinasc.naturalmae.ID'))       # Foreign key
    naturalmae = relationship('NATURALMAE')
    CODMUNNATU_ID = Column(String(6), ForeignKey('sinasc.codmunnatu.ID'))       # Foreign key
    codmunnatu = relationship('CODMUNNATU')
    ESCMAE2010_ID = Column(String(2), ForeignKey('sinasc.escmae2010.ID'))       # Foreign key
    escmae2010 = relationship('ESCMAE2010')
    DTNASCMAE = Column(Date)                                                    # Atributo
    RACACORMAE_ID = Column(String(2), ForeignKey('sinasc.racacormae.ID'))       # Foreign key
    racacormae = relationship('RACACORMAE')
    QTDGESTANT = Column(Float)                                                  # Atributo
    QTDPARTNOR = Column(Float)                                                  # Atributo
    QTDPARTCES = Column(Float)                                                  # Atributo
    IDADEPAI = Column(Float)                                                    # Atributo
    DTULTMENST = Column(Date)                                                   # Atributo
    SEMAGESTAC = Column(Float)                                                  # Atributo
    TPMETESTIM_ID = Column(String(2), ForeignKey('sinasc.tpmetestim.ID'))       # Foreign key
    tpmetestim = relationship('TPMETESTIM')
    CONSPRENAT = Column(Float)                                                  # Atributo
    MESPRENAT = Column(String(2))                                               # Atributo
    TPAPRESENT_ID = Column(String(2), ForeignKey('sinasc.tpapresent.ID'))       # Foreign key
    tpapresent = relationship('TPAPRESENT')
    STTRABPART_ID = Column(String(2), ForeignKey('sinasc.sttrabpart.ID'))       # Foreign key
    sttrabpart = relationship('STTRABPART')
    STCESPARTO_ID = Column(String(2), ForeignKey('sinasc.stcesparto.ID'))       # Foreign key
    stcesparto = relationship('STCESPARTO')
    TPNASCASSI_ID = Column(String(2), ForeignKey('sinasc.tpnascassi.ID'))       # Foreign key
    tpnascassi = relationship('TPNASCASSI')
    TPFUNCRESP_ID = Column(String(2), ForeignKey('sinasc.tpfuncresp.ID'))       # Foreign key
    tpfuncresp = relationship('TPFUNCRESP')
    TPDOCRESP = Column(String(5))                                               # Atributo
    DTDECLARAC = Column(Date)                                                   # Atributo
    ESCMAEAGR1_ID = Column(String(2), ForeignKey('sinasc.escmaeagr1.ID'))       # Foreign key
    escmaeagr1 = relationship('ESCMAEAGR1')
    STDNEPIDEM = Column(Numeric)                                                # Atributo
    STDNNOVA = Column(Numeric)                                                  # Atributo
    CODPAISRES_ID = Column(String(3), ForeignKey('sinasc.codpaisres.ID'))       # Foreign key
    codpaisres = relationship('CODPAISRES')
    TPROBSON_ID = Column(String(2), ForeignKey('sinasc.tprobson.ID'))           # Foreign key
    tprobson = relationship('TPROBSON')
    PARIDADE = Column(Numeric)                                                  # Atributo
    KOTELCHUCK = Column(String(2))                                              # Atributo
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos
class CODESTAB(Base):
    # Nome da tabela no banco de dados
    __tablename__ = 'codestab'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(7), primary_key=True)                                    # Primary key
    DESCESTAB = Column(String(66))                                              # Logical key
    ESFERA = Column(String(66))                                                 # Atributo
    REGIME = Column(String(66))                                                 # Atributo

# Tabela dos municípios de nascimento
class CODMUNNASC(Base):
    __tablename__ = 'codmunnasc'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sinasc.ufcod.ID'))                 # Foreign key
    ufcod = relationship('UFCOD')
    AMAZONIA = Column(String(66))                                               # Atributo
    FRONTEIRA = Column(String(66))                                              # Atributo
    CAPITAL = Column(String(66))                                                # Atributo
    LATITUDE = Column(Float)                                                    # Atributo
    LONGITUDE = Column(Float)                                                   # Atributo
    ALTITUDE = Column(Float)                                                    # Atributo
    AREA = Column(Float)                                                        # Atributo
    ANOINST = Column(String(66))                                                # Atributo
    ANOEXT = Column(String(66))                                                 # Atributo
    SUCESSOR = Column(String(66))                                               # Atributo

# Tabela dos locais de nascimento
class LOCNASC(Base):
    __tablename__ = 'locnasc'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    LOCAL = Column(String(66))                                                  # Logical key

# Tabela de estados civis da mãe
class ESTCIVMAE(Base):
    __tablename__ = 'estcivmae'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    SITUACAO = Column(String(66))                                               # Logical key

# Tabela das faixas de anos de instrução da mãe
class ESCMAE(Base):
    __tablename__ = 'escmae'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    FAIXA_DE_ANOS_INSTRUCAO = Column(String(66))                                # Logical key

# Tabela das ocupações da mãe
class CODOCUPMAE(Base):
    __tablename__ = 'codocupmae'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Tabela dos municípios de residência da mãe
class CODMUNRES(Base):
    __tablename__ = 'codmunres'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sinasc.ufcod.ID'))                 # Foreign key
    ufcod = relationship('UFCOD')
    AMAZONIA = Column(String(66))                                               # Atributo
    FRONTEIRA = Column(String(66))                                              # Atributo
    CAPITAL = Column(String(66))                                                # Atributo
    LATITUDE = Column(Float)                                                    # Atributo
    LONGITUDE = Column(Float)                                                   # Atributo
    ALTITUDE = Column(Float)                                                    # Atributo
    AREA = Column(Float)                                                        # Atributo
    ANOINST = Column(String(66))                                                # Atributo
    ANOEXT = Column(String(66))                                                 # Atributo
    SUCESSOR = Column(String(66))                                               # Atributo

# Tabela das faixas de semanas de gestação
class GESTACAO(Base):
    __tablename__ = 'gestacao'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    FAIXA_DE_SEMANAS_GESTACAO = Column(String(66))                              # Logical key

# Tabela das multiplicidades da gestação
class GRAVIDEZ(Base):
    __tablename__ = 'gravidez'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    MULTIPLICIDADE_GESTACAO = Column(String(66))                                # Logical key

# Tabela dos tipos de parto
class PARTO(Base):
    __tablename__ = 'parto'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela das faixas do número de consultas
class CONSULTAS(Base):
    __tablename__ = 'consultas'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    FAIXA_DE_NUMERO_CONSULTAS = Column(String(66))                              # Logical key

# Tabela dos tipos de raça do RN
class RACACOR(Base):
    __tablename__ = 'racacor'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos tipos de anomalia do RN
class CODANOMAL(Base):
    __tablename__ = 'codanomal'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    ANOMALIA = Column(String(66))                                               # Logical key

# Tabela do local de nascimento da mãe
class NATURALMAE(Base):
    __tablename__ = 'naturalmae'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    LOCAL = Column(String(66))                                                  # Logical key

# Tabela dos municípios de nascimento da mãe
class CODMUNNATU(Base):
    __tablename__ = 'codmunnatu'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sinasc.ufcod.ID'))                 # Foreign key
    ufcod = relationship('UFCOD')
    AMAZONIA = Column(String(66))                                               # Atributo
    FRONTEIRA = Column(String(66))                                              # Atributo
    CAPITAL = Column(String(66))                                                # Atributo
    LATITUDE = Column(Float)                                                    # Atributo
    LONGITUDE = Column(Float)                                                   # Atributo
    ALTITUDE = Column(Float)                                                    # Atributo
    AREA = Column(Float)                                                        # Atributo
    ANOINST = Column(String(66))                                                # Atributo
    ANOEXT = Column(String(66))                                                 # Atributo
    SUCESSOR = Column(String(66))                                               # Atributo

# Tabela das escolaridades da mãe
class ESCMAE2010(Base):
    __tablename__ = 'escmae2010'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESCOLARIDADE = Column(String(66))                                           # Logical key

    # Tabela dos tipos de raça da mãe
class RACACORMAE(Base):
    __tablename__ = 'racacormae'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO = Column(String(66))                                                   # Logical key

# Tabela dos métodos utilizados para saber o número de semanas de gestação
class TPMETESTIM(Base):
    __tablename__ = 'tpmetestim'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    METODO = Column(String(66))                                                 # Logical key

# Tabela dos tipos de posição do RN
class TPAPRESENT(Base):
    __tablename__ = 'tpapresent'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    POSICAO = Column(String(66))                                                # Logical key

# Tabela da indução ou não do parto
class STTRABPART(Base):
    __tablename__ = 'sttrabpart'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    INDUCAO = Column(String(66))                                                # Logical key

# Tabela se a cesárea ocorreu ou não antes do parto iniciar
class STCESPARTO(Base):
    __tablename__ = 'stcesparto'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CESAREA_ANTES_PARTO = Column(String(66))                                    # Logical key

# Tabela dos tipos de profissional que assistem parto
class TPNASCASSI(Base):
    __tablename__ = 'tpnascassi'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ASSISTENCIA = Column(String(66))                                            # Logical key

# Tabela dos tipos de função do responsável pelo preenchimento da DN
class TPFUNCRESP(Base):
    __tablename__ = 'tpfuncresp'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    FUNCAO = Column(String(66))                                                 # Logical key

# Tabela das escolaridades da mãe
class ESCMAEAGR1(Base):
    __tablename__ = 'escmaeagr1'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESCOLARIDADE = Column(String(66))                                           # Logical key

# Tabela do país de nascimento
class CODPAISRES(Base):
    __tablename__ = 'codpaisres'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(3), primary_key=True)                                    # Primary key
    PAIS = Column(String(66))                                                   # Logical key

# Tabela da classificação de Robson
class TPROBSON(Base):
    __tablename__ = 'tprobson'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    DESCRICAO = Column(String(66))                                              # Logical key

# Tabela dos Estados da RFB
class UFCOD(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'sinasc'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'sinasc'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
