{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE ViewPatterns    #-}
-- |Helper functions for printing QNFs
module Data.QnfQuery.AShow (ashowQNFVoid,qnfToQuery) where


import           Control.Monad
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Foldable
import           Data.Functor.Identity
import           Data.List
import           Data.QnfQuery.Build
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Unsafe

ashowQNFVoid
  :: (Functor dbg_f,AShow (dbg_f (Query () ())))
  => QNFQuerySPDCF sel_f prod_f dbg_f col_f f e s
  -> SExp
ashowQNFVoid QNFQuery {..} =
  Rec "QNFQuery" [("qnfOrigDEBUG'",ashow' $ bivoid <$> qnfOrigDEBUG')]

-- | Note that this is a very naive funciton that returns just an e if
-- it can or Nothing if e is more complex.
qnfToQuery
  :: (Foldable sel_f,Foldable prod_f,Foldable f)
  => QNFQuerySPDCF sel_f prod_f dbg_f Either f e s
  -> Query (Maybe e) s
qnfToQuery QNFQuery {..} = proj
  where
    proj = case qnfColumns of
      Left (toList -> cols)
        -> Q1 (QProj ((Nothing,) <$> fmap2 nameToSym cols)) sel
      Right (toList -> aggCols,toList -> comb) -> Q1
        (QGroup
           ((Nothing,) . E0 <$> fmap2 (E0 . nameToSym) aggCols)
           (fmap2 nameToSym comb))
        sel
    sel = case toList qnfSel of
      [] -> prod
      xs -> S (foldl1 And $ fmap4 dropSel xs) prod
    prod =
      foldl1' (Q2 QProd)
      $ join . bimap nameToSym (either Q0 qnfToQuery) . head . toList
      <$> toList qnfProd

dropSel :: QNFSelName e s -> Maybe e
dropSel = \case
  Left e  -> Just e
  Right x -> case runIdentity $ getConst $ qnfColumns x of
    E0 name -> nameToSym name
    _       -> Nothing

nameToSym :: QNFNameSPC sel_f prod_f Either e s -> Maybe e
nameToSym = \case
  PrimaryCol e _s _id -> Just e
  Column col _id      -> dropCol col
  NonSymbolName e     -> Just e

dropCol :: QNFColSPC sel_f prod_f Either e s -> Maybe e
dropCol col =
  either (dropProj . runIdentity) (dropAggr . runIdentity . fst)
  $ qnfColumns col

dropAggr :: Aggr (QNFName e s) -> Maybe e
dropAggr (NAggr AggrFirst e) = nameToSym e
dropAggr _                   = Nothing

dropProj :: Expr (QNFName e s) -> Maybe e
dropProj (E0 n) = nameToSym n
dropProj _      = Nothing

test :: Query (Maybe Int) [Int]
test =
  qnfToQuery
  $ toQNF_test
  $ Q1 (QGroup [(10,1 .* 2)] [])
  $ J (1 .= 2) [1] [2,3]

(.=) :: e -> e -> Prop (Rel (Expr e))
a .= b = atom $ R2 REq (atom2 a) (atom2 b)

(.*) :: e -> e -> Expr (Aggr (Expr e))
a .* b = E0 $ NAggr AggrFirst $ E2 EMul (E0 a) (E0 b)

toQNF_test :: (AShowV e,Hashables1 e) => Query e [e] -> QNFQuery e [e]
toQNF_test q =
  fst $ head $ fromRightErr $ (`evalStateT` def) $ runListT $ toQNF Just q
