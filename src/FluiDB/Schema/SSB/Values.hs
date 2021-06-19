module FluiDB.Schema.SSB.Values (ssbGlobalConf,T,N) where

-- | graphGlobalConf $ mkGraphSchema [[(1,2)]]
import           Data.Bifunctor
import           Data.Query.SQL.FileSet
import           Data.Query.SQL.Types
import           Data.Utils.Functors
import           Data.Utils.Unsafe
import           FluiDB.ConfValues
import           FluiDB.Schema.SSB.Queries
import           FluiDB.Types
import           Text.Printf

type T = ()
type N = ()

ssbGlobalConf :: GlobalConf ExpTypeSym Table T N
ssbGlobalConf =
  fromJustErr
  $ mkGlobalConf
    (id,id) -- expIso
    genUniqName -- Build a unique name.
    (Just . DataFile . datFile)
    (first TSymbol <$> fmap3 ESym ssbPrimKeys) -- Primary keys
    (bimap TSymbol (_) <$> ssbFldSchema) -- Full schema
    _tableSizeAssoc -- sizes in pages
  where
    genUniqName i = \case
      ESym e -> Just $ ESym $ printf "uniq_%s_%d" e i
      _      -> Nothing


datFile :: Table -> FilePath
datFile = printf "%s/tables/%s.dat" resourcesDir . unTable

resourcesDir :: FilePath
#ifdef __APPLE__
resourcesDir = "/Users/drninjabatman/Projects/UoE/FluiDB/resources/"
#else
resourcesDir = "/home/drninjabatman/Projects1/FluiDB/resources/"
#endif
