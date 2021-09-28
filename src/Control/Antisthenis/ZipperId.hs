module Control.Antisthenis.ZipperId
  (ZipperId(..)
  ,ZipperName
  ,zidModTrail
  ,zidNamed
  ,zidReset
  ,zidDefault
  ,zidBumpVersion) where


import           Data.List
import           Data.String
import           Data.Utils.AShow
type ZipperName = String
-- zidTrail is just the trail that leads to this zipper being
-- revolved. Each zipper can only revolve at a single trail which
-- evolutionControl returns none. the zidTrail should be erased when
-- it returns a value.
zidModTrail :: ([ZipperName] -> [ZipperName]) -> ZipperId -> ZipperId
zidDefault :: String -> ZipperId
zidBumpVersion :: ZipperId -> Maybe ZipperId
zidReset :: ZipperId -> ZipperId

type ZipperNames = [String]
data ZipperId =
  ZipperId
  { zidName :: ZipperName,zidTrail :: ZipperNames,zidVersion :: Maybe Int }
instance IsString ZipperId where
  fromString = zidDefault
zidModTrail f zid = zid { zidTrail = f $ zidTrail zid }
zidReset zid = zid { zidVersion = Just 0 }
zidDefault name = ZipperId { zidName = name,zidTrail = [],zidVersion = Just 0 }
instance AShow ZipperId where
  ashow' ZipperId {..} =
    sexp (zidName ++ ":" ++ show zidVersion) [] -- $ Sym <$> zidTrail
zidBumpVersion zid = case zidVersion zid of
  Just v  -> Just $ zid { zidVersion = Just $ 1 + v }
  Nothing -> Nothing
zidNamed :: ZipperId -> String -> Bool
zidNamed zid = (`isSuffixOf` zidName zid)
