module FluiDB.Bamify.Main (bamifyMain) where
import           Control.Monad
import           Data.Query.QuerySize
import           Data.Utils.Functors
import           FluiDB.Bamify.CsvParse
import           FluiDB.Schema.SSB.Queries
import           System.Environment

bamifySingleSSBFile :: SSBTable -> FilePath -> FilePath -> IO TableSize
bamifySingleSSBFile name tblFile bamaFile = do
  case fmap2 fst $ lookup name ssbSchema of
    Nothing    -> fail $ "No schema found for " ++ show (name,fst <$> ssbSchema)
    Just types -> bamifyFile types tblFile bamaFile

bamifyMain :: IO ()
bamifyMain = void $ getArgs >>= \case
  [tblName,tblFile,bamaFile] -> bamifySingleSSBFile tblName tblFile bamaFile
  args -> fail
    $ "expected args: <tbl-name> <tbl-file> <bama-file>. Got: " ++ show args
