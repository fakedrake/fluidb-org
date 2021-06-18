module FluiDB.Bamify.Types
  (ParseErr
  ,DBGenTableConf(..)
  ,DBGenConf(..)
  ,CppCode
  ,TblFile
  ,DataFile
  ,BamaFile
  ,MP) where
import qualified Data.ByteString.Lazy         as BS
import qualified Data.ByteString.Lazy.Char8   as BSC
import           Data.Query.QuerySchema.Types
import           Data.Void
import           Text.Megaparsec

type ParseErr = ParseErrorBundle BSC.ByteString Void

data DBGenTableConf =
  DBGenTableConf
  { dbGenTableConfFileBase :: FilePath
   ,dbGenTableConfChar     :: Char
  }

data DBGenConf =
  DBGenConf
  { dbGenConfExec        :: FilePath
   ,dbGenConfTables      :: [DBGenTableConf]
   ,dbGenConfScale       :: Float -- 1 means 1GB
   ,dbGenConfIncremental :: Bool
   ,dbGenConfSchema      :: [(String,CppSchema)]
  }

type TblFile = FilePath
type DataFile = FilePath
type BamaFile = FilePath
type CppCode = FilePath
type MP a = Parsec Void BS.ByteString a
