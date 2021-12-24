module FluiDB.Schema.SSB.Values (getSsbGlobalConf,T,N) where

import           Data.Bifunctor
import           Data.Query.QuerySize
import           Data.Query.SQL.QFile
import           Data.Query.SQL.Types
import           Data.Utils.AShow
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
        ,pgcBudget=Just 65000 -- Scaling: 0.01, remember to change build-workload.sh
        ,pgcExpIso=(id,id)
        ,pgcToUniq=genUniqName
        ,pgcToQFile= \case
            TSymbol s ->Just $ DataFile $ dataDir </> s <.> "dat"
            NoTable   -> Nothing
        ,pgcDataDir = "/tmp/fluidb_store"
        }
  case retM of
    Left l  -> fail $ ashow l
    Right r -> return r
  where
    genUniqName i = \case
      ESym e -> Just $ ESym $ printf "uniq_%s_%d" e i
      _      -> Nothing


ssbDBGen :: FilePath -> IO [(Table,TableSize)]
ssbDBGen dataDir = do
  let createParents = True
  createDirectoryIfMissing createParents dataDir
  withCurrentDirectory dataDir
    $ mkAllDataFiles
    $ ssbTpchDBGenConf
    $ fst <$> ssbSchema
