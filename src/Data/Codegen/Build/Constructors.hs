{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing #-}

module Data.Codegen.Build.Constructors
  ( Constr
  , ConstructorArg(..)
  , clusterCall
  , getQueriesFromClust
  , sortCall
  , tmplClass
  , constrArgs
  , opStatements
  , opConstructor
  , selCall
  , prodCall
  , unionCall
  , dropCall
  , limitCall
  , groupCall
  , constrBlock
  ) where

import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Propagators
import           Data.Cluster.Types
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build.Classes
import           Data.Codegen.Build.EquiJoinUtils
import           Data.Codegen.Build.Expression
import           Data.Codegen.Build.IoFiles.Types
import           Data.Codegen.Build.Monads
import           Data.Codegen.Schema
import qualified Data.CppAst                      as CC
import           Data.Either
import           Data.Foldable
import           Data.List
import           Data.Maybe
import           Data.Monoid
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.QFile
import qualified Data.Set                         as DS
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Tup
import           Prelude                          hiding (exp)

type Constr = (CC.FunctionSymbol CC.CodeSymbol, [CC.TmplInstArg CC.CodeSymbol])

tmplClass :: CC.Class a -> CC.TmplInstArg a
tmplClass = CC.TypeArg . CC.ClassType mempty [] . CC.className

-- mkProjection<PrimFn, SecFn>
projCall
  :: forall e s t n m c .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> QProjI (ShapeSym e s)
  -> [(ShapeSym e s,Expr (ShapeSym e s))]
  -> m Constr
projCall UnFwd _ QProjNoInv pr =
  throwAStr $ "No join inversion for projection: " ++ ashow pr
projCall UnFwd _ QProjInv {..} pr = do
  primFn <- projToFn $ fmap3 Right pr
  -- Is it ok that we are not translating the symbols?
  coFn <- projToFn [(e,E0 $ Right e) | e <- qpiCompl]
  return ("mkProjection",tmplClass <$> [primFn,coFn])
projCall UnRev outSh ipr _pr = do
  Tup2 prim sec <- getQueries @Tup2 @e @s
  joinQueryCall ForwardTrigger
    $ EasyJClust
    { ejcJoin = oaShape outSh
     ,ejcIO = swap <$> oaIoAssoc outSh
     ,ejcP = toEqProp $ qpiOverlap ipr
     ,ejcLeft = prim
     ,ejcRight = sec
    }

toEqProp :: [(e,e)] -> Prop (Rel (Expr e))
toEqProp (x:xs) = foldl' (\p x' -> And (toEq x') p) (toEq x) xs where
  toEq (e,e') = P0 (R2 REq (R0 (E0 e)) (R0 (E0 e')))
toEqProp _ = error "Empty eq prop for projection"

-- mkSort<ToKeyFn>
sortCall
  :: forall e s t n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Identity e s m)
  => OutShape e s
  -> [Expr (ShapeSym e s)]
  -> m Constr
sortCall OutShape {oaIoAssoc = outAssoc} sr = do
  toKeyFn <- exprSetFn =<< traverse2 (translateSym $ swap <$> outAssoc)  sr
  return ("mkSort",tmplClass <$> [toKeyFn])


exprSetFn
  :: (MonadCodeCheckpoint e s t n m,MonadSchemaScope Identity e s m)
  => [Expr (ShapeSym e s)]
  -> m (CC.Class CC.CodeSymbol)
exprSetFn sr = do
  keySch :: CppSchema <- forM (zip [0 :: Int ..] sr) $ \(i,exp) -> do
    ty <- exprCppTypeErr exp
    let sym = fromString $ "sortElem" ++ show i
    return (ty,sym)
  -- Key record class
  keyRec <- tellRecordCls keySch
  let keyRecSym = CC.classNameRef keyRec
  -- Expressions comprising the key
  exprs <- runListT $ do
    exp <- mkListT $ return sr
    lift $ exprExpression $ fmap Right exp
  -- Class function to make key
  tellClassM
    $ mkCallClass (CC.ClassType mempty [] keyRecSym)
    $ CC.FunctionAp (CC.SimpleFunctionSymbol keyRecSym) [] exprs

-- mkAggregation<FoldFn :: From -> To, StartNew> There is no reverse
-- trigger for groupCall
groupCall
  :: forall e s t n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Identity e s m)
  => [(ShapeSym e s,Expr (Aggr (Expr (ShapeSym e s))))]
  -> [Expr (ShapeSym e s)]
  -> m Constr
groupCall pr g = do
  codomainCls <- tellRecordCls =<< cppSchema =<< aggrSchema @Identity @e @s pr
  aggrExpressionEither <- aggrProjExpression pr
  foldCls <- tellClassM $ do
    noAggrDecl <- mkCallClass
      -- Codomain
      (CC.ClassType mempty [] $ CC.classNameRef codomainCls)
      -- Expression (domain inferred)
      $ fmap
        (either (CC.runSymbol . CC.declarationNameRef) id)
        aggrExpressionEither
    let aggrVars = lefts $ toList aggrExpressionEither
    return
      noAggrDecl
      { CC.classPrivateMembers =
          fmap CC.MemberVariable aggrVars ++ CC.classPrivateMembers noAggrDecl
      }
  grpKeyFn <- exprSetFn g
  return ("mkAggregation",tmplClass <$> [foldCls,grpKeyFn])

-- mkDrop<Rec, i>
limitCall
  :: forall e s t n m c .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> Int
  -> m Constr
limitCall UnRev io _i = unionCall io
limitCall UnFwd _ i = do
  qrec <- queryRecord . runIdentity =<< getQueries
  return ("mkLimit", [CC.TemplArg $ CC.LiteralIntExpression i, tmplClass qrec])

-- mkLimit<i, Rec>
dropCall
  :: forall e s t c n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> Int
  -> m Constr
dropCall UnFwd _io i = first (const "mkDrop") <$> limitCall UnFwd undefined i
dropCall UnRev io _i = unionCall io

#if 0
-- EquiJoin<LExtractFn, RExtractFn, Combine>
-- mkJoin<Pred, Comb>
--
-- Note that the C++ library knows that the left and right triangles
-- are the same as the inputs so it reuses those classes. If name
-- erasure for unique names here works properly this should be
-- producing functional code.
joinCall
  :: forall e s t n c m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => BinDir c
  -> OutShape e s
  -> Prop (Rel (Expr (ShapeSym e s)))
  -> m Constr
-- idAssocAndShape is the output shape and id associations for
-- all syms.
joinCall BinRev _ _ = do
  Identity jshape <- getQueries @Identity @e @s
  return ("mkUnJoin",tmplClass <$> [extrFnL,extrFnR])
joinCall BinFwd jShape p = do
  -- Note: A `Rantij` A == A `Lantij` A makes duplicate output columns
  -- so don't expect output symbols to be unique within the list.
  combFn <- combinerClass jShape
  pInSyms <- traverse3 (translateSym $ swap <$> oaIoAssoc jShape) p
  joinPred <- equiJoinPred pInSyms
  fmap3 tmplClass $ case joinPred of
    Just eqClss -> do
      EquiJoinPred {..} <- mkEquiJoinPred eqClss
      return ("mkEquiJoin",[equiJoinLExtract,equiJoinRExtract,combFn])
    Nothing -> do
      propCls <- propCallClass pInSyms
      return ("mkJoin",[propCls,combFn])
#endif

-- mkUnion<R>
unionCall
  :: forall e s t n m c .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Tup2 e s m)
  => OutShape e s
  -> m Constr
unionCall OutShape {oaIoAssoc = assoc} = do
  Tup2 shapeL shapeR <- getQueries @Tup2 @e @s
  Tup2 projL projR0 <- traverse (symsProj . querySchema) $ Tup2 shapeL shapeR
  aassert
    (isSubsetOf (fst <$> projR0) (fst <$> projL))
    "Union should be extending the second with nulls."
  let projR =
        fmap3 Right projR0
        ++ [(e,E0 $ Left ty) | e@(_,ty)
           <- setDiff (fst <$> projL) (fst <$> projR0)]
  fromL <- evalQueryEnv (Identity shapeL)
    $ projToFn
    $ first fst <$> fmap3 Right projL
  fromR <- evalQueryEnv (Identity shapeR) $ projToFn $ first fst <$> projR
  return ("mkUnion",tmplClass <$> [fromL,fromR])
  where
    setDiff :: Eq a => [a] -> [a] -> [a]
    setDiff xs ys = filter (`notElem` ys) xs
    isSubsetOf :: Eq a => [a] -> [a] -> Bool
    isSubsetOf xs ys = all (`elem` ys) xs
    symsProj :: [(CC.CppType,ShapeSym e s)]
             -> m [((ShapeSym e s,CC.CppType),Expr (ShapeSym e s))]
    symsProj = mapM $ \(ty,s) -> case lookup s assoc of
      Nothing   -> throwAStr $ "Missing assoc for symbol: " ++ ashow (s,assoc)
      Just sOut -> return ((sOut,ty),E0 s)

-- mkProduct<Combine>
prodCall
  :: forall e s t n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Tup2 e s m)
  => OutShape e s
  -> m Constr
prodCall outAssocAndShape = do
  combFn <- combinerClass outAssocAndShape
  return ("mkProduct", [tmplClass combFn])

-- Select<Pred, From>
selCall
  :: forall c e s t n m .
  (MonadCodeCheckpoint e s t n m,Traversable c,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> Prop (Rel (Expr (ShapeSym e s)))
  -> m Constr
selCall UnRev outSh _ = unionCall outSh
selCall UnFwd OutShape{oaIoAssoc=outAssoc} p = do
  propCls <- propCallClass =<< traverse3 (translateSym $ fmap swap outAssoc) p
  return ("mkSelect", [tmplClass propCls])


getQueriesFromClust
  :: (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => AnyCluster e s t n
  -> m [QNFQuery e s]
getQueriesFromClust clust = getNodeQnfN $ snd $ primaryNRef clust


-- | Auxiliary construct for easily passing information to the join
-- constructor.
data EasyJClust e s =
  EasyJClust
  { ejcJoin,ejcLeft,ejcRight :: QueryShape e s
  -- antijoins can be handled implicitly because they have the same
  -- shape as the inputs..
  --
 -- ,ejcLAnti,ejcRAnti :: QueryShape e s
   ,ejcIO                    :: [(ShapeSym e s,ShapeSym e s)]
   ,ejcP                     :: Prop (Rel (Expr (ShapeSym e s)))
  }


type PropJoinClust e s t n =
  JoinClust'
    (WMetaD (Defaulting (QueryShape e s)) NodeRef)
    (WMetaD
       [BQOp (ShapeSym e s)]
       (WMetaD (Defaulting (QueryShape e s)) NodeRef))
    t
    n
-- | Extract the components of a join regardless of the direction.
mkEasyJoin
  :: (MonadReader (ClusterConfig e s t n) m,MonadCodeCheckpoint e s t n m)
  => [(ShapeSym e s,ShapeSym e s)]
  -> PropJoinClust e s t n
  -> m (EasyJClust e s)
mkEasyJoin ejcIO JoinClust {..} = do
  ejcJoin <- getShape $ binClusterOut joinBinCluster
  ejcLeft <- getShape $ binClusterLeftIn joinBinCluster
  ejcRight <- getShape $ binClusterRightIn joinBinCluster
  let outSyms = shapeAllSyms ejcJoin
  op <- selectOp (const "No valid join op") (checkBOp outSyms)
    $ binClusterOut joinBinCluster
  case op of
    QJoin ejcP -> return EasyJClust { .. }
    _          -> throwAStr $ "Expected join but got: " ++ ashow op

getShape :: MonadAShowErr e s err m
         => WMetaD a (WMetaD (Defaulting (QueryShape e s)) f) b
         -> m (QueryShape e s)
getShape (WMetaD (_,WMetaD (a,_))) =
  maybe (throwAStr "empty defaulting") return $ getDef a

-- | get the inputs, evalQueryEnv with them and copy the queryCall
-- stuff. The fact that we have a
clusterCall
  :: forall e s t n m .
  (MonadReader (ClusterConfig e s t n) m,MonadCodeCheckpoint e s t n m)
  => Direction
  -> AnyCluster e s t n
  -> m Constr
clusterCall dir c = do
  (ACPropagatorAssoc {acpaInOutAssoc = assoc}
    ,shapeClust) <- getValidClustPropagator c
  go shapeClust assoc c
  where
    go :: ShapeCluster NodeRef e s t n
       -> [(ShapeSym e s,ShapeSym e s)]
       -> AnyCluster e s t n
       -> m Constr
    go shapeClust assoc = const $ case shapeClust of
      JoinClustW jc -> joinQueryCall dir =<< mkEasyJoin assoc jc
      BinClustW BinClust {..} -> do
        -- The default shapes have been synchronized and are ready to
        -- go.
        l <- getShape binClusterLeftIn
        r <- getShape binClusterRightIn
        outShape <- getShape binClusterOut
        op <- selectOp
          (ashow . (,assoc))
          (checkBOp $ shapeAllSyms outShape)
          binClusterOut
        case dir of
          ReverseTrigger -> evalQueryEnv (Identity outShape)
            $ bopQueryCall
              BinRev
              OutShape { oaIoAssoc = swap <$> assoc,oaShape = l }
              op
          ForwardTrigger -> evalQueryEnv (Tup2 l r)
            $ bopQueryCall
              BinFwd
              OutShape { oaIoAssoc = assoc,oaShape = outShape }
              op
      UnClustW UnClust {..} -> do
        inShape <- getShape unClusterIn
        -- Remember we are making the primary one here
        primOutShape <- getShape unClusterPrimaryOut
        op <- selectOp
          (ashow . (,assoc))
          (checkUOp (shapeAllSyms inShape) (shapeAllSyms primOutShape))
          unClusterPrimaryOut
        case dir of
          ReverseTrigger -> do
            secOutShape <- getShape unClusterSecondaryOut
            evalQueryEnv (Tup2 primOutShape secOutShape)
              $ uopQueryCall
                UnRev
                OutShape { oaIoAssoc = swap <$> assoc,oaShape = inShape }
                op
          ForwardTrigger -> evalQueryEnv (Identity inShape)
            $ uopQueryCall
              UnFwd
              OutShape { oaIoAssoc = assoc,oaShape = primOutShape }
              op
      NClustW (NClust (WMetaD (_,ref))) ->
        throwCodeErr $ ForwardCreateSymbol ref

-- | Check that the symbol is in the list.
selem :: Hashables2 e s => ShapeSym e s -> [ShapeSym e s] -> Bool
selem sym syms = case shapeSymQnfName sym of
  NonSymbolName _ -> True
  _               -> sym `elem` syms

-- | Check that the op can correspond to the pair of in symbols/our
-- symbols. Used to find valid operations.
checkUOp
  :: Hashables2 e s
  => [ShapeSym e s]
  -> [ShapeSym e s]
  -> UQOp (ShapeSym e s)
  -> Bool
checkUOp inSyms outSyms = \case
  QSel p -> all3 (`selem` outSyms) p
  QProj _ pr -> all (`selem` outSyms) (fst <$> pr)
    && all (`selem` inSyms) (toList3 pr)
  QGroup pr es -> all (`selem` outSyms) (fst <$> pr)
    && all (`selem` inSyms) (toList3 $ toList3 pr)
    && all (`selem` inSyms) (toList2 es)
  QSort es -> all2 (`selem` outSyms) es
  QLimit _ -> True
  QDrop _ -> True

-- | Check that the op can correspond to the pair of in symbols/our
-- symbols. Used to find valid operations.
checkBOp :: Hashables2 e s => [ShapeSym e s] -> BQOp (ShapeSym e s) -> Bool
checkBOp outSyms = \case
  QJoin p          -> all3 (`selem` outSyms) p
  QLeftAntijoin p  -> all3 (`selem` outSyms) p
  QRightAntijoin p -> all3 (`selem` outSyms) p
  QProd            -> True
  QUnion           -> True
  QProjQuery       -> True
  QDistinct        -> True

-- | Find an op that matches the predicate.
selectOp
  :: MonadAShowErr e s err m
  => ((AShowV e,AShowV s) => [f (ShapeSym e s)] -> String)
  -> (f (ShapeSym e s) -> Bool)
  -> WMetaD [f (ShapeSym e s)] g n
  -> m (f (ShapeSym e s))
selectOp msg checkOp (WMetaD (ops,_)) = case filter checkOp ops of
  []   -> throwAStr $ "No matching operator on output: " ++ msg ops
  op:_ -> return op

-- | Emits the constructor for a join query. EasyJClust can output any
-- _direct_ mapping (ie no expression, just a permutation) of the
-- input columns to the output. This is used both for joining and for
-- projections assuming that we do not need to invert the projection
-- the projections.
joinQueryCall
  :: MonadCodeCheckpoint e s t n m => Direction -> EasyJClust e s -> m Constr
joinQueryCall dir EasyJClust {..} = case dir of
  ReverseTrigger -> evalQueryEnv (Identity ejcJoin) $ do
    -- Remember: there are two outputs per input symbol in join.
    lproj <- sequenceA
      [(i,) . E0 <$> translateSym (reverse ejcIO) i | i <- shapeAllSyms ejcLeft]
    rproj <- sequenceA
      [(i,) . E0 <$> translateSym ejcIO i | i <- shapeAllSyms ejcRight]
    extractlFn <- projToFn $ fmap3 Right lproj
    extractrFn <- projToFn $ fmap3 Right rproj
    return ("mkUnJoin",tmplClass <$> [extractlFn,extractrFn])
  ForwardTrigger -> evalQueryEnv (Tup2 ejcLeft ejcRight) $ do
    -- Note: A `Rantij` A == A `Lantij` A makes duplicate output columns
    -- so don't expect output symbols to be unique within the list.
    combFn <- combinerClass OutShape { oaIoAssoc = ejcIO,oaShape = ejcJoin }
    pInSyms <- traverse3 (translateSym $ swap <$> ejcIO) ejcP
    joinPred <- equiJoinPred pInSyms
    fmap3 tmplClass $ case joinPred of
      Just eqClss -> do
        EquiJoinPred {..} <- mkEquiJoinPred eqClss
        return ("mkEquiJoin",[equiJoinLExtract,equiJoinRExtract,combFn])
      Nothing -> do
        propCls <- propCallClass pInSyms
        return ("mkJoin",[propCls,combFn])

bopQueryCall
  :: forall e s t n c m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => BinDir c
  -> OutShape e s
  -> BQOp (ShapeSym e s)
  -> m Constr
bopQueryCall dir outAssocAndShape = \case
  QJoin _ -> throwAStr "QJoin should be handled as a separate cluster type."
  QDistinct -> throwAStr "QDistinct should have been optimized out."
  QProjQuery -> throwAStr "QProjQuery should have been optimized out."
  QUnion -> case dir of
    BinRev -> error "union does not have a reverse."
    BinFwd -> unionCall outAssocAndShape
  QProd -> case dir of
    BinRev -> error "We do not deal with reverse products"
    BinFwd -> prodCall outAssocAndShape
  QLeftAntijoin _p -> error
    "should have been handled by joinQueryCall joinCall dir outAssocAndShape p"
  QRightAntijoin _p -> error
    "should have been handled by joinQueryCall joinCall dir outAssocAndShape p"

uopQueryCall
  :: forall e s t n c m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> UQOp (ShapeSym e s)
  -> m Constr
uopQueryCall dir outSh op = case op of
  -- Remember that the symbols of selection refer to the out shape.
  QSel p -> selCall dir outSh p
  QGroup pr g -> case dir of
    UnRev -> error "Can't reverse trigger the group call"
    UnFwd -> groupCall pr g
  QProj inv pr -> projCall dir outSh inv pr
  QSort sr -> case dir of
    UnRev -> error "Cant reverse trigger sort"
    UnFwd -> sortCall outSh sr
  QLimit i -> limitCall dir outSh i
  QDrop i -> dropCall dir outSh i

-- | From the constructor and the file constelation make a c++
-- expression that creates the operation.
opConstructor
  :: MonadCodeError e s t n m
  => Constr
  -> IOFilesD e s
  -> m (CC.Expression CC.CodeSymbol)
opConstructor (name,tmpl) ioFiles =
  CC.FunctionAp name tmpl <$> constrArgs ioFiles

-- | Construct an operator, run it and print the output.
opStatements :: CC.Expression CC.CodeSymbol -> [CC.Statement CC.CodeSymbol]
opStatements ctor = [
  CC.DeclarationSt (CC.Declaration "operation" "auto") $ Just ctor
  , member "operation" "run" []
  , member "operation" "print_output" [CC.LiteralIntExpression 10]]
  where
    member :: CC.Expression CC.CodeSymbol
           -> CC.FunctionSymbol CC.CodeSymbol
           -> [CC.Expression CC.CodeSymbol]
           -> CC.Statement CC.CodeSymbol
    member exp mem arg = CC.ExpressionSt
                         $ CC.FunctionAp (CC.InstanceMember exp mem) [] arg

class ConstructorArg a where
  -- |Provide a value to get the c++ type (c++ types do not have to
  -- match haskell types). In some cases we don't have a value but we
  -- need a type.
  toConstrType :: Maybe a -> CC.Type CC.CodeSymbol
  toConstrArg :: a -> CC.Expression CC.CodeSymbol

instance ConstructorArg x => ConstructorArg (Identity x) where
  toConstrType = toConstrType . fmap runIdentity
  toConstrArg = toConstrArg . runIdentity
instance ConstructorArg a => ConstructorArg (Maybe (QueryShape e s), a) where
  toConstrType (Just (_sh,a)) = toConstrType $ Just a
  toConstrType Nothing        = toConstrType (Nothing :: Maybe a)
  toConstrArg = toConstrArg . snd
instance ConstructorArg a => ConstructorArg (Maybe a) where
  toConstrType :: forall a . ConstructorArg a => Maybe (Maybe a) -> CC.Type CC.CodeSymbol
  toConstrType = \case
    Nothing -> constr "Nothing" (Nothing :: Maybe a)
    Just x  -> constr "Just" x
    where
      constr constrName constrArg = CC.ClassType
        (DS.singleton CC.CppConst)
        [CC.TypeArg $ toConstrType constrArg]
        (fromString constrName)

  toConstrArg :: forall a . ConstructorArg a => Maybe a -> CC.Expression CC.CodeSymbol
  toConstrArg x = toConstrArgMaybeExp x $ toConstrArg <$> x

toConstrArgMaybeExp
  :: ConstructorArg a
  => Maybe a
  -> Maybe (CC.Expression CC.CodeSymbol)
  -> CC.Expression CC.CodeSymbol
toConstrArgMaybeExp forType = \case
  Just x  -> CC.FunctionAp "Just" [CC.TypeArg $ toConstrType forType] [x]
  -- constr "Just" (Just x) [x]
  Nothing -> CC.FunctionAp "Nothing" [CC.TypeArg $ toConstrType forType] []

instance ConstructorArg QFile where
  toConstrType = \case
    Nothing           -> toConstrType (Nothing :: Maybe FilePath)
    Just (DataFile f) -> toConstrType $ Just f
  toConstrArg (DataFile x) = toConstrArg x

instance ConstructorArg FilePath where
  toConstrType _ = CC.constString
  toConstrArg = CC.LiteralStringExpression

-- | Create the constructor args for an operator out of the file
-- cluster.
--
-- Reverse triggering join clusters does not require all outputs to be materialized
constrArgs
  :: forall e s t n m .
  MonadCodeError e s t n m
  => IOFilesD e s
  -> m [CC.Expression CC.CodeSymbol]
constrArgs ioFiles = do
  outAndIn <- toIoTup ioFiles
  let nothing =  CC.FunctionAp "Nothing" [] []
  let just e = CC.FunctionAp "Just" [] [e]
  case (iofCluster ioFiles,iofDir ioFiles,outAndIn) of
    (JoinClustW _,ReverseTrigger,([Just l,Just r],[Just o,Just lo,Just ro])) ->
      return $ just <$> [o,lo,ro,l,r] -- mkUnJoin
    (JoinClustW _,ReverseTrigger,([Just i,Nothing],[Just o,Just lo,Nothing])) ->
      return [just o,just lo,nothing,just i,nothing]
    (JoinClustW _,ReverseTrigger,([Nothing,Just i],[Just o,Nothing,Just ro])) ->
      return [just o,nothing,just ro,nothing,just i]
    (JoinClustW _,ReverseTrigger,oi) ->
      -- XXX: We need to check that the generated tables are actually
      -- created due to triggering the node or if they just happened
      -- to be materialized
      throwAStr $ "outs ins: " ++ ashow oi
    (_,_,(outs,inps')) ->
      case ((toConstrArgMaybeExp (Nothing :: Maybe QFile) <$> outs) ++)
      <$> sequenceA inps' of
        Nothing -> throwAStr "oops"
        Just x  -> return x

-- Tup2 outs inps' = toFiles ioFiles
toIoTup
  :: MonadCodeError e s t n m
  => IOFilesD e s
  -> m
    ([Maybe (CC.Expression CC.CodeSymbol)]
    ,[Maybe (CC.Expression CC.CodeSymbol)])
toIoTup ioFiles = catch $ case iofCluster ioFiles of
  JoinClustW JoinClust {..} -> mktup
    (iofDir ioFiles)
    [binClusterOut joinBinCluster
    ,joinClusterLeftAntijoin
    ,joinClusterRightAntijoin]
    [binClusterLeftIn joinBinCluster,binClusterRightIn joinBinCluster]
  BinClustW BinClust {..} ->
    mktup (iofDir ioFiles) [binClusterOut] [binClusterLeftIn,binClusterRightIn]
  UnClustW UnClust {..} -> mktup
    (iofDir ioFiles)
    [unClusterPrimaryOut,unClusterSecondaryOut]
    [unClusterIn]
  NClustW _c -> error "ncluster arguments requested: "
  where
    catch m = catchError m $ \err -> throwAStr
      $ "Error in toIoTup:" ++ ashow (ashowIOFiles $ iofCluster ioFiles,err)

mktup
  :: forall e s t n m ops .
  MonadCodeError e s t n m
  => Direction
  -> [WMetaD
       ops
       Identity
       (NodeRole
          (NodeRef ())
          (MaybeBuild (Maybe (QueryShape e s),FilePath))
          (MaybeBuild (Maybe (QueryShape e s),QFile)))]
  -> [WMetaD
       ops
       Identity
       (NodeRole
          (NodeRef ())
          (MaybeBuild (Maybe (QueryShape e s),FilePath))
          (MaybeBuild (Maybe (QueryShape e s),QFile)))]
  -> m
    ([Maybe (CC.Expression CC.CodeSymbol)]
    ,[Maybe (CC.Expression CC.CodeSymbol)])
mktup dir fwdOuts fwdIns = (,) <$> traverse mkOut outs <*> traverse mkIn ins
  where
    (outs,ins) = case dir of
      ForwardTrigger -> (fwdOuts,fwdIns)
      ReverseTrigger -> (fwdIns,fwdOuts)
    mkIn,mkOut
      :: (ConstructorArg o,ConstructorArg i)
      => WMetaD a Identity (NodeRole interm (MaybeBuild i) (MaybeBuild o))
      -> m (MaybeBuild (CC.Expression CC.CodeSymbol))
    mkOut (WMetaD (_,Identity (Output a))) = return $ toConstrArg <$> a
    mkOut (WMetaD (_,Identity b)) =
      throwAStr $ "Expected output node but got something else: " ++ ashow x
      where
        x = mapIntermRole (const ()) $ bimap (const ()) (const ()) b
    mkIn (WMetaD (_,Identity (Input a))) = return $ toConstrArg <$> a
    mkIn _ = throwAStr "Expected input node but got something else"

-- | First come up witha an expression for building the constructor
-- and the build the actual expressions.
constrBlock
  :: MonadCodeError e s t n m
  => Constr
  -> IOFilesD e s
  -> m (CC.Statement CC.CodeSymbol)
constrBlock c iof = CC.Block . opStatements <$> opConstructor c iof
