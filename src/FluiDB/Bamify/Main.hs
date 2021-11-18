module FluiDB.Bamify.Main (bamifyMain) where
import           Control.Monad
import           Data.Bifunctor
import           Data.List
import           Data.Query.QuerySchema.Types
import           Data.String
import           FluiDB.Bamify.CsvParse
import           FluiDB.Bamify.Unbamify
import           FluiDB.Classes               (IOOps (ioFileExists))
import           FluiDB.Schema.SSB.Queries
import           System.Directory
import           System.Environment
import           System.FilePath.Posix
import           System.Posix.ByteString      (fileExist)
import           Text.Printf

getSsbSchema :: SSBTable ->  IO CppSchema
getSsbSchema name = do
  case lookup name ssbSchema of
    Nothing    -> fail $ "No schema found for " ++ show (name,fst <$> ssbSchema)
    Just schema -> return $ second fromString <$>  schema

bamifyMain :: IO ()
bamifyMain = void $ getArgs >>= \case
  [_exe,tblDir,bamaDir,dataDir] -> do
    tbls <- fmap dropExtensions . filter (".tbl" `isSuffixOf`)
      <$> listDirectory tblDir
    forM_ tbls $ \tbl -> do
      let tblFile = tblDir </> tbl <.> "tbl"
      let bamaFile = bamaDir </> tbl <.> "bama"
      let datFile = dataDir </> tbl <.> "dat"
      bamaExists <- doesPathExist bamaFile
      datExists <- doesPathExist bamaFile
      schema <- getSsbSchema tbl
      unless bamaExists $ do
        putStrLn $ printf "Bamify-ing %s -> %s [table:%s]" tblFile bamaFile tbl
        void $ bamifyFile (fst <$> schema) tblFile bamaFile
      unless datExists $ do
        putStrLn $ printf "Unbamify-ing %s -> %s [table:%s]" bamaFile datFile tbl
        mkDataFile schema bamaFile datFile
  args ->
    fail $ "expected args: <tbl-dir> <bama-dir> <data-dir>. Got: " ++ show args
