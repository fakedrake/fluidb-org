module FluiDB.Schema.SSB.Values (ssbGlobalConf,T,N) where

-- | graphGlobalConf $ mkGraphSchema [[(1,2)]]
import           Control.Monad
import           Data.Bifunctor
import           Data.Query.QuerySize
import           Data.Query.SQL.FileSet
import           Data.Query.SQL.Types
import           Data.Utils.Functors
import           Data.Utils.Unsafe
import           FluiDB.Bamify.DBGen
import           FluiDB.ConfValues
import           FluiDB.Schema.SSB.Queries
import           FluiDB.Types
import           System.Directory
import           System.FilePath
import           Text.Printf

type T = ()
type N = ()

ssbGlobalConf :: IO (GlobalConf ExpTypeSym Table T N)
ssbGlobalConf = do
  tmp <- getTemporaryDirectory
  let dataDir = tmp </> "fluidb-data"
  ssbDBGen dataDir
  let retM = mkGlobalConf PreGlobalConf
        {pgcPrimKeyAssoc=first TSymbol <$> fmap3 ESym ssbPrimKeys
        ,pgcSchemaAssoc=bimap TSymbol (fmap2 ESym) <$> ssbSchema
        ,pgcTableSizeAssoc=error "not implemented"
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

tableSizeAssoc :: [(Table,TableSize)]
tableSizeAssoc = error "not implemented"

datFile :: Table -> FilePath
datFile = printf "%s/tables/%s.dat" resourcesDir . unTable

ssbDBGen :: FilePath -> IO ()
ssbDBGen dataDir = do
  let createParents = True
  createDirectoryIfMissing createParents dataDir
  withCurrentDirectory dataDir $ do
    mkAllDataFiles $ ssbTpchDBGenConf $ fst <$> ssbSchema
    tables <- listDirectory dataDir
    putStrLn "Created tables:"
    forM_ tables $ \tbl -> putStrLn $ "\t" ++ tbl

resourcesDir :: FilePath
#ifdef __APPLE__
resourcesDir = "/Users/drninjabatman/Projects/UoE/FluiDB/resources/"
#else
resourcesDir = "/home/drninjabatman/Projects1/FluiDB/resources/"
#endif
