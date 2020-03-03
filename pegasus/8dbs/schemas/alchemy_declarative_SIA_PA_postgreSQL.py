###########################################################################################################################################################################
# SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA #
###########################################################################################################################################################################

from sqlalchemy import Column, ForeignKey, String, Numeric, Integer, BigInteger, Float, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


"""
Cria o schema do banco de dados do SIA_PA (Procedimentos Ambulatoriais) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Incorpora o schema do banco de dados do SIA_PA definido nas classes abaixo
Base = declarative_base()

# Tabela Procedimentos Ambulatoriais (PA) (tabela principal do "sub" banco de dados SIA_PA)
class PABR(Base):
    __tablename__ = 'pabr'
    __table_args__ = {'schema': 'sia_pa'}
    # Aqui se define as colunas para a tabela spbr (o mesmo é realizado com as outras "class")
    # Note que cada coluna é também um atributo de instância (o mesmo se aplica às outras "class")
    UF_PA = Column(String(2))                                                   # Atributo
    ANO_PA = Column(Integer)                                                    # Atributo
    MES_PA = Column(String(2))                                                  # Atributo
    PACODUNI_ID = Column(String(7), ForeignKey('sia_pa.pacoduni.ID'))           # Foreign key
    pacoduni = relationship('PACODUNI')
    PAGESTAO_ID = Column(String(6), ForeignKey('sia_pa.pagestao.ID'))           # Foreign key
    pagestao = relationship('PAGESTAO')
    PACONDIC_ID = Column(String(2), ForeignKey('sia_pa.pacondic.ID'))           # Foreign key
    pacondic = relationship('PACONDIC')
    PAUFMUN_ID = Column(String(6), ForeignKey('sia_pa.paufmun.ID'))             # Foreign key
    paufmun = relationship('PAUFMUN')
    PAREGCT_ID = Column(String(4), ForeignKey('sia_pa.paregct.ID'))             # Foreign key
    paregct = relationship('PAREGCT')
    PA_INCOUT = Column(Numeric)                                                 # Atributo
    PA_INCURG = Column(Numeric)                                                 # Atributo
    PATPUPS_ID = Column(String(2), ForeignKey('sia_pa.patpups.ID'))             # Foreign key
    patpups = relationship('PATPUPS')
    PATIPPRE_ID = Column(String(2), ForeignKey('sia_pa.patippre.ID'))           # Foreign key
    patippre = relationship('PATIPPRE')
    PA_MN_IND = Column(String(2))                                               # Atributo
    PA_CNPJCPF = Column(String(14))                                             # Atributo
    PA_CNPJMNT = Column(String(14))                                             # Atributo
    PA_CNPJ_CC = Column(String(14))                                             # Atributo
    PA_MVM = Column(Date)                                                       # Atributo
    PA_CMP = Column(Date)                                                       # Atributo
    PAPROC_ID = Column(String(10), ForeignKey('sia_pa.paproc.ID'))              # Foreign key
    paproc = relationship('PAPROC')
    PATPFIN_ID = Column(String(2), ForeignKey('sia_pa.patpfin.ID'))             # Foreign key
    patpfin = relationship('PATPFIN')
    PANIVCPL_ID = Column(String(2), ForeignKey('sia_pa.panivcpl.ID'))           # Foreign key
    panivcpl = relationship('PANIVCPL')
    PADOCORIG_ID = Column(String(2), ForeignKey('sia_pa.padocorig.ID'))         # Foreign key
    padocorig = relationship('PADOCORIG')
    PA_AUTORIZ = Column(String(13))                                             # Atributo
    PA_CNSMED = Column(String(15))                                              # Atributo
    PACBOCOD_ID = Column(String(6), ForeignKey('sia_pa.pacbocod.ID'))           # Foreign key
    pacbocod = relationship('PACBOCOD')
    PAMOTSAI_ID = Column(String(2), ForeignKey('sia_pa.pamotsai.ID'))           # Foreign key
    pamotsai = relationship('PAMOTSAI')
    PA_OBITO = Column(Numeric)                                                  # Atributo
    PA_ENCERR = Column(Numeric)                                                 # Atributo
    PA_PERMAN = Column(Numeric)                                                 # Atributo
    PA_ALTA = Column(Numeric)                                                   # Atributo
    PA_TRANSF = Column(Numeric)                                                 # Atributo
    PACIDPRI_ID = Column(String(4), ForeignKey('sia_pa.pacidpri.ID'))           # Foreign key
    pacidpri = relationship('PACIDPRI')
    PACIDSEC_ID = Column(String(4), ForeignKey('sia_pa.pacidsec.ID'))           # Foreign key
    pacidsec = relationship('PACIDSEC')
    PACIDCAS_ID = Column(String(4), ForeignKey('sia_pa.pacidcas.ID'))           # Foreign key
    pacidcas = relationship('PACIDCAS')
    PACATEND_ID = Column(String(2), ForeignKey('sia_pa.pacatend.ID'))           # Foreign key
    pacatend = relationship('PACATEND')
    PA_IDADE = Column(Integer)                                                  # Atributo
    IDADEMIN = Column(Integer)                                                  # Atributo
    IDADEMAX = Column(Integer)                                                  # Atributo
    PAFLIDADE_ID = Column(String(2), ForeignKey('sia_pa.paflidade.ID'))         # Foreign key
    paflidade = relationship('PAFLIDADE')
    PASEXO_ID = Column(String(2), ForeignKey('sia_pa.pasexo.ID'))               # Foreign key
    pasexo = relationship('PASEXO')
    PARACACOR_ID = Column(String(2), ForeignKey('sia_pa.paracacor.ID'))         # Foreign key
    paracacor = relationship('PARACACOR')
    PAMUNPCN_ID = Column(String(6), ForeignKey('sia_pa.pamunpcn.ID'))           # Foreign key
    pamunpcn = relationship('PAMUNPCN')
    PA_QTDPRO = Column(Integer)                                                 # Atributo
    PA_QTDAPR = Column(Integer)                                                 # Atributo
    PA_VALPRO = Column(Float)                                                   # Atributo
    PA_VALAPR = Column(Float)                                                   # Atributo
    PA_UFDIF = Column(Numeric)                                                  # Atributo
    PA_MNDIF = Column(Numeric)                                                  # Atributo
    PA_DIF_VAL = Column(Float)                                                  # Atributo
    NU_VPA_TOT = Column(Float)                                                  # Atributo
    NU_PA_TOT = Column(Float)                                                   # Atributo
    PAINDICA_ID = Column(String(2), ForeignKey('sia_pa.paindica.ID'))           # Foreign key
    paindica = relationship('PAINDICA')
    PACODOCO_ID = Column(String(2), ForeignKey('sia_pa.pacodoco.ID'))           # Foreign key
    pacodoco = relationship('PACODOCO')
    PA_FLER = Column(Numeric)                                                   # Atributo
    PAETNIA_ID = Column(String(4), ForeignKey('sia_pa.paetnia.ID'))             # Foreign key
    paetnia = relationship('PAETNIA')
    PA_VL_CF = Column(Float)                                                    # Atributo
    PA_VL_CL = Column(Float)                                                    # Atributo
    PA_VL_INC = Column(Float)                                                   # Atributo
    PASRCC_ID = Column(String(6), ForeignKey('sia_pa.pasrcc.ID'))               # Foreign key
    pasrcc = relationship('PASRCC')
    PAINE_ID = Column(String(10), ForeignKey('sia_pa.paine.ID'))                # Foreign key
    paine = relationship('PAINE')
    PANATJUR_ID = Column(String(4), ForeignKey('sia_pa.panatjur.ID'))           # Foreign key
    panatjur = relationship('PANATJUR')
    CONTAGEM = Column(BigInteger, primary_key=True)                             # Primary Key

# Tabela dos estabelecimentos de saúde
class PACODUNI(Base):
    __tablename__ = 'pacoduni'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(7), primary_key=True)                                    # Primary key
    DESCESTAB = Column(String(66))                                              # Logical key
    RSOC_MAN = Column(String(66))                                               # Atributo
    CPF_CNPJ = Column(String(14))                                               # Atributo
    EXCLUIDO = Column(Numeric)                                                  # Atributo
    DATAINCL = Column(Date)                                                     # Atributo
    DATAEXCL = Column(Date)                                                     # Atributo

# Tabela dos municípios de gestão
class PAGESTAO(Base):
    __tablename__ = 'pagestao'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sia_pa.ufcod.ID'))                 # Foreign key
    ufcod = relationship('UFCOD_PA')
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

# Tabela dos tipos de gestão
class PACONDIC(Base):
    __tablename__ = 'pacondic'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    GESTAO = Column(String(66))                                                 # Logical key

# Tabela dos municípios de localização do estabelecimento de saúde
class PAUFMUN(Base):
    __tablename__ = 'paufmun'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sia_pa.ufcod.ID'))                 # Foreign key
    ufcod = relationship('UFCOD_PA')
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

# Tabela dos tipos de regra contratuual
class PAREGCT(Base):
    __tablename__ = 'paregct'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    REGRA = Column(String(66))                                                  # Logical key

# Tabela dos tipos de estabelecimento
class PATPUPS(Base):
    __tablename__ = 'patpups'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTABELECIMENTO = Column(String(66))                                        # Logical key

# Tabela dos tipos de prestador
class PATIPPRE(Base):
    __tablename__ = 'patippre'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESFERA = Column(String(66))                                                 # Logical key

# Tabela dos tipos de procedimento
class PAPROC(Base):
    __tablename__ = 'paproc'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(10), primary_key=True)                                   # Primary key
    PROCEDIMENTO = Column(String(100))                                          # Logical key

# Tabela dos tipos de financiamento
class PATPFIN(Base):
    __tablename__ = 'patpfin'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    FINANCIAMENTO = Column(String(66))                                          # Logical key

# Tabela dos níveis de complexidade
class PANIVCPL(Base):
    __tablename__ = 'panivcpl'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    COMPLEXIDADE = Column(String(66))                                           # Logical key

# Tabela dos tipos de sub-bancos de dados do SIA
class PADOCORIG(Base):
    __tablename__ = 'padocorig'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    TIPO_SIA = Column(String(66))                                               # Logical key

# Tabela das ocupações
class PACBOCOD(Base):
    __tablename__ = 'pacbocod'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Tabela dos motivos de saída
class PAMOTSAI(Base):
    __tablename__ = 'pamotsai'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    MOTIVO = Column(String(66))                                                 # Logical key

# Tabela dos diagnósticos primários
class PACIDPRI(Base):
    __tablename__ = 'pacidpri'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    DIAGNOSTICO = Column(String(66))                                            # Logical key

# Tabela dos diagnósticos secundários
class PACIDSEC(Base):
    __tablename__ = 'pacidsec'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    DIAGNOSTICO = Column(String(66))                                            # Logical key

# Tabela dos diagnósticos associados
class PACIDCAS(Base):
    __tablename__ = 'pacidcas'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    DIAGNOSTICO = Column(String(66))                                            # Logical key

# Tabela dos tipos de caráter do atendimento
class PACATEND(Base):
    __tablename__ = 'pacatend'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    CARATER = Column(String(66))                                                # Logical key

# Tabela dos tipos de compatibilidade de idade
class PAFLIDADE(Base):
    __tablename__ = 'paflidade'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    COMPATIBILIDADE = Column(String(66))                                        # Logical key

# Tabela dos tipos de sexo
class PASEXO(Base):
    __tablename__ = 'pasexo'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    SEXO = Column(String(66))                                                   # Logical key

# Tabela dos tipos de raça
class PARACACOR(Base):
    __tablename__ = 'paracacor'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    RACA = Column(String(66))                                                   # Logical key

# Tabela dos municípios de residência do paciente
class PAMUNPCN(Base):
    __tablename__ = 'pamunpcn'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    MUNNOME	= Column(String(66))                                                # Logical key
    MUNNOMEX = Column(String(66))                                               # Atributo
    MUNCODDV = Column(String(7))                                                # Atributo
    OBSERV = Column(String(66))                                                 # Atributo
    SITUACAO = Column(String(66))                                               # Atributo
    MUNSINP	= Column(String(66))                                                # Atributo
    MUNSIAFI = Column(String(66))                                               # Atributo
    UFCOD_ID = Column(String(2), ForeignKey('sia_pa.ufcod.ID'))                 # Foreign key
    ufcod = relationship('UFCOD_PA')
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

# Tabela dos tipos de situação da produção
class PAINDICA(Base):
    __tablename__ = 'paindica'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    SITUACAO = Column(String(66))                                               # Logical key

# Tabela dos tipos de ocorrência da produção
class PACODOCO(Base):
    __tablename__ = 'pacodoco'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    OCORRENCIA = Column(String(66))                                             # Logical key

# Tabela dos tipos de etnia indígena
class PAETNIA(Base):
    __tablename__ = 'paetnia'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    INDIGENA = Column(String(66))                                               # Logical key

# Tabela das ocupações
class PASRCC(Base):
    __tablename__ = 'pasrcc'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(6), primary_key=True)                                    # Primary key
    OCUPACAO = Column(String(66))                                               # Logical key

# Tabela dos tipos de equipes
class PAINE(Base):
    __tablename__ = 'paine'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(10), primary_key=True)                                   # Primary key
    EQUIPE = Column(String(66))                                                 # Logical key

# Tabela dos tipos de equipes
class PANATJUR(Base):
    __tablename__ = 'panatjur'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(4), primary_key=True)                                    # Primary key
    NATUREZA = Column(String(66))                                               # Logical key

# Tabela dos Estados da RFB
class UFCOD_PA(Base):
    __tablename__ = 'ufcod'
    __table_args__ = {'schema': 'sia_pa'}
    ID = Column(String(2), primary_key=True)                                    # Primary key
    ESTADO = Column(String(66))                                                 # Logical key
    SIGLA_UF = Column(String(66))                                               # Atributo

# Tabela de Arquivos
class ARQUIVOS_PA(Base):
    __tablename__ = 'arquivos'
    __table_args__ = {'schema': 'sia_pa'}
    NOME = Column(String(15), primary_key=True)                                 # Primary key
    DIRETORIO = Column(String(66))                                              # Atributo
    DATA_INSERCAO_FTP = Column(Date)                                            # Atributo
    DATA_HORA_CARGA = Column(DateTime)                                          # Atributo
    QTD_REGISTROS = Column(Integer)                                             # Atributo
