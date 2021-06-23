module FluiDB.Schema.SSB.Values (getSsbGlobalConf,T,N) where

import           Control.Monad
import           Data.Bifunctor
import           Data.Query.QuerySize
import           Data.Query.SQL.FileSet
import           Data.Query.SQL.Types
import           Data.Utils.Functors
import           FluiDB.Bamify.DBGen
import           FluiDB.ConfValues
import           FluiDB.Schema.SSB.Queries
import           FluiDB.Types
import           System.Directory
import           System.FilePath
import           Text.Printf

type T = ()
type N = ()

getSsbGlobalConf :: IO (GlobalConf ExpTypeSym Table T N)
getSsbGlobalConf = do
  tmp <- getTemporaryDirectory
  let dataDir = tmp </> "fluidb-data"
  sizes <- ssbDBGen dataDir
  let retM = mkGlobalConf PreGlobalConf
        {pgcPrimKeyAssoc=first TSymbol <$> fmap3 ESym ssbPrimKeys
        ,pgcSchemaAssoc=bimap TSymbol (fmap2 ESym) <$> ssbSchema
        ,pgcTableSizeAssoc=sizes
        ,pgcExpIso=(id,id)
        ,pgcToUniq=genUniqName
        ,pgcToFileSet= \case
            TSymbol s ->Just $ DataFile $ dataDir </> s <.> "dat"
            NoTable   -> Nothing
        }
  maybe (fail "mkGlobalConf failed") return retM
  where
    genUniqName i = \case
      ESym e -> Just $ ESym $ printf "uniq_%s_%d" e i
      _      -> Nothing


ssbDBGen :: FilePath -> IO [(Table,TableSize)]
ssbDBGen dataDir = do
  let createParents = True
  createDirectoryIfMissing createParents dataDir
  withCurrentDirectory dataDir $ do
    ret <- mkAllDataFiles $ ssbTpchDBGenConf $ fst <$> ssbSchema
    tables <- listDirectory dataDir
    putStrLn "Created tables:"
    forM_ tables $ \tbl -> putStrLn $ "\t" ++ tbl
    return ret
