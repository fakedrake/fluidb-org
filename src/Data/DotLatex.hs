{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE ViewPatterns          #-}
{-# OPTIONS_GHC -Wno-unused-top-binds -Wno-name-shadowing -Wno-orphans #-}

module Data.DotLatex
  ( Latex(..)
  , NodeType(..)
  , Latexifiable(..)
  , simpleRender
  ) where

import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Coerce
import           Data.List
import qualified Data.List.NonEmpty     as NEL
import           Data.Maybe
import           Data.Monoid
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.Query.SQL.FileSet
import           Data.Query.SQL.Types
import           Data.String
import           Data.Utils.Default
import           Data.Utils.Functors
import           Prelude                hiding (filter, lookup)
import           System.FilePath.Posix
import           Text.Printf

-- # DOT
data Shape
  = Square
  | Box
  | Ellipse
  deriving Show
data Style
  = Invis
  | Filled
  | Wedged
  deriving Show
-- http://graphviz.org/doc/info/colors.html
data Color1
  = Red
  | Blue
  | Green
  | Purple
  | Orange
  | Yellow
  | Brown
  | Pink
  | Grey
  deriving (Enum,Show)
data Color
  = NoColor
  | MultiColor (NEL.NonEmpty Color1)
  | SingleColor Color1
  deriving Show
type NodeLabel = String
data DotNodeProp = DLabel NodeLabel
  | DShape Shape
  | DBackground Color
  | DStyle Style
  deriving Show
newtype DotEdgeProp = DDir IsDirected deriving Show
type DSym = String
data IsDirected = Directed | Undirected deriving (Show, Eq)
data DProperty
  = GlobalEdgeProp [DotEdgeProp]
  | GlobalNodeProp [DotNodeProp]
  | GlobalTitle String
  deriving (Show)
data DotAst
  = DLink (DSym,DSym) [DotEdgeProp]
  | DNode DSym [DotNodeProp]
  | DSubGraph DSym [DotAst]
  | DGraph DSym [DotAst]
  | DProp DProperty
  | DLiteral String
  deriving Show

instance Monoid Color where
  mempty = NoColor
instance Semigroup Color where
  NoColor <> x = x
  x <> NoColor = x
  (SingleColor c) <> (SingleColor c') = MultiColor $ c NEL.:| [c']
  (SingleColor c) <> (MultiColor (c' NEL.:| cs')) =
    MultiColor (c NEL.:| c':cs')
  (MultiColor (c' NEL.:| cs')) <> (SingleColor c)  =
    MultiColor (c' NEL.:| cs' ++ [c])
  (MultiColor (c NEL.:| cs)) <> (MultiColor (c' NEL.:| cs')) =
    MultiColor (c NEL.:| cs ++ (c':cs'))

newtype DotRenderSettings = DotRenderSettings {
  structureDirected :: IsDirected
  } deriving Show

instance Default DotRenderSettings where
  def = DotRenderSettings Directed

type RenderM = Reader DotRenderSettings
runRenderM :: DotRenderSettings -> RenderM x -> x
runRenderM = flip runReader

instance Semigroup (RenderM String) where
  a <> b = mappend <$> a <*> b
instance Monoid (RenderM String) where
  mempty = return mempty

toDotProps :: ToDot a => [a] -> RenderM String
toDotProps [] = return ""
toDotProps ps = printf "[%s]" . intercalate ", " <$> traverse toDot' ps

-- | If there are multiple color properties merge them and put the
-- correct style property (wedged for multpile colors, filled for just
-- one).
sanitized :: [DotNodeProp] -> [DotNodeProp]
sanitized = putStyle . mergeColors
  where
    putStyle (x@(MultiColor _),rst)  = DStyle Wedged : DBackground x : rst
    putStyle (x@(SingleColor _),rst) = DStyle Filled : DBackground x : rst
    putStyle (NoColor,rst)           = rst
    mergeColors =
      first (mconcat . mapMaybe justCol)
      . partition
        (\case
           DBackground _ -> True
           _             -> False)
      where
        justCol (DBackground x) = Just x
        justCol _               = Nothing


class ToDot a where
  toDot' :: a -> RenderM String

instance ToDot Color1 where
  toDot' = return . ("/set19/" ++) . show . (+1) . fromEnum
instance ToDot Color where
  toDot' = \case
    MultiColor cs -> fmap (intercalate ":") $ traverse toDot' $ NEL.toList cs
    SingleColor c -> toDot' c
    NoColor       -> return "white"
instance ToDot Style where
  toDot' = return . \case
    Filled -> "filled"
    Invis  -> "invis"
    Wedged -> "wedged"
instance ToDot Shape where
  toDot' = return . \case
    Box     -> "box"
    Square  -> "square"
    Ellipse -> "ellipse"

escapeQuotes :: String -> String
escapeQuotes = \case
  []           -> []
  '"':xs       -> "\\\"" ++ escapeQuotes xs
  '\\':'\"':xs -> '\\':'\"':escapeQuotes xs
  x:xs         -> x:escapeQuotes xs

instance ToDot DotNodeProp where
  toDot' = \case
    DLabel l      -> return $ "label=\"" ++ escapeQuotes l ++ "\""
    DShape s      -> ("shape=" ++) <$> toDot' s
    DBackground c -> printf "fillcolor=\"%s\"" <$> toDot' c
    DStyle s      -> ("style=" ++) <$> toDot' s

instance ToDot DotEdgeProp where
  toDot' = return . \case
    DDir Directed   -> "dir=forward"
    DDir Undirected -> "dir=none"

instance ToDot [DotEdgeProp] where
  toDot' = toDotProps

instance ToDot [DotNodeProp] where
  toDot' = toDotProps . sanitized

instance ToDot DSym where
  toDot' = return

instance ToDot [DotAst] where
  toDot' = fmap concat . fmap2 (++ ";\n") . traverse toDot'

instance ToDot DProperty where
  toDot' = \case
    GlobalEdgeProp xs -> printf "edge%s" <$> toDot' xs
    GlobalNodeProp xs -> printf "node%s" <$> toDot' xs
    GlobalTitle xs    -> printf "label=\"%s\"" <$> toDot' (escapeQuotes xs)

instance ToDot [DProperty] where
  toDot' = fmap concat . fmap2 (++ "\n") . traverse toDot'

instance ToDot DotAst where
  toDot' = \case
    DLink (x, y) p -> mconcat [toDot' x, arr, toDot' y, toDot' p] where
      arr = do
        i <- asks structureDirected
        return $ if i == Directed then " -> " else " -- "
    DNode s p -> toDot' s <> toDot' p
    DSubGraph s p -> printf "subgraph %s {\n%s}" s <$> toDot' p
    DLiteral x -> return x
    DProp x -> toDot' x
    DGraph s p -> do
      isDir <- asks ((== Directed) . structureDirected)
      let gtype = if isDir then "digraph" else "graph" :: String
      body <- toDot' p
      return $ printf "%s %s {\n%s}" gtype s body

-- renderLinks :: GraphBuilderT DSym DSym Identity [DotAst]
-- renderLinks = do
--   (lRefs,rRefs) <- nodeRefs
--   lLinks <- mkLinks L (fmap toNodeList . getAllLinksT Out) $ toNodeList lRefs
--   rLinks <- mkLinks R (fmap toNodeList . getAllLinksN Inp) $ toNodeList rRefs
--   return $ lLinks ++ rLinks
--   where
--     mkLinks side getN refs = runListT $ do
--       r <- mkListT $ return refs
--       n <- mkListT $ getN r
--       return $ DLink (toSym side r,toSym (otherSide side) n) []
renderLinks :: GraphBuilderT DSym DSym Identity [DotAst]
renderLinks = do
  g <- gets gbPropNet
  return
    [case nldNSide of
      Out -> DLink (toSym L nldTNode,toSym R nldNNode) []
      Inp -> DLink (toSym R nldNNode,toSym L nldTNode) []
    | NodeLinkDescr {..} <- listLinks g]


data LatexSide = L | R deriving Show
otherSide :: LatexSide -> LatexSide
otherSide L = R
otherSide R = L
toSym :: LatexSide -> NodeRef a -> DSym
toSym s = printf "node%s%s" (show s) . show . fromEnum

data BipartiteRenderHead = BipartiteRenderHead {
  globalHead :: [DotAst],
  leftHead   :: [DotAst],
  rightHead  :: [DotAst],
  edgeHead   :: [DotAst]
  }

instance Default BipartiteRenderHead where
  def = BipartiteRenderHead
    []
    [DProp (GlobalNodeProp [DShape Box])]
    [DProp (GlobalNodeProp [DShape Ellipse])]
    [DProp (GlobalEdgeProp [DDir Directed])]

renderAll
  :: BipartiteRenderHead
  -> [(NodeRef NodeLabel
       -> Bool
      ,Color1)]
  -> [(NodeRef NodeLabel
       -> Bool
      ,Color1)]
  -> DSym
  -> GraphBuilderT NodeLabel NodeLabel Identity DotAst
renderAll BipartiteRenderHead {..} csetl csetr s = do
  links <- renderLinks
  (fmap (fixN L) . toNodeList -> lnodes
    ,fmap (fixN R) . toNodeList -> rnodes) <- nodeRefs
  return
    $ DGraph s
    $ globalHead ++ rightHead ++ rnodes ++ lnodes ++ edgeHead ++ links
  where
    getShape = \case
      R -> Ellipse
      L -> Box
    fixN s r =
      DNode (toSym s r)
      $ [DLabel $ printf "%s" (show r),DShape (getShape s)] ++ color s r
    csetFn L = csetl
    csetFn R = csetr
    color :: LatexSide -> NodeRef NodeLabel -> [DotNodeProp]
    color s r = case snd <$> filter (($ r) . fst) (csetFn s) of
      []   -> []
      [x]  -> [DBackground $ SingleColor x]
      x:xs -> [DBackground $ MultiColor $ x NEL.:| xs]

data NodeType = Op | N
newtype Latex (a :: NodeType) = Latex {unLatex :: String} deriving Show

instance Semigroup (Latex a) where
  a <> b = Latex $ unLatex a <> unLatex b
instance Monoid (Latex a) where
  mempty = Latex mempty

instance IsString (Latex a) where
  fromString = Latex

castLatex :: Latex a -> Latex b
castLatex = Latex . unLatex


parens :: Latex a -> Latex a
parens x = "(" <> x <> ")"

spaces :: [Latex a] -> Latex a
spaces = Latex . unwords . fmap unLatex

comma :: [Latex a] -> Latex a
comma = Latex . intercalate ", " . fmap unLatex

class Latexifiable x where
  toLatex :: forall y. x -> Latex y

instance Latexifiable () where
  toLatex () = "\\varnothing"
instance Latexifiable Int where
  toLatex = Latex . show
instance Latexifiable Integer where
  toLatex = Latex . show
instance Latexifiable String where
  toLatex = Latex
instance Latexifiable FileSet where
  toLatex = \case
    DataFile x -> Latex $ takeBaseName x
    DataAndSet x y -> Latex
      $ printf "(%s, %s)" (takeBaseName x) (takeBaseName y)

instance Latexifiable Char where
  toLatex x = Latex [x]

instance (Latexifiable a, Latexifiable b) => Latexifiable (a,b) where
  toLatex (x,y) = Latex $ printf "(%s, %s)" (go x) (go y) where
    go :: forall x . Latexifiable x => x -> String
    go = unLatex . toLatex

instance Latexifiable BEOp where
  toLatex = \case
    EEq   -> "="
    ELike -> "\\tilde"
    EAnd  -> "\\land"
    EOr   -> "\\lor"
    ENEq  -> "\\neq"
    EAdd  -> "+"
    ESub  -> "-"
    EMul  -> "\\dot"
    EDiv  -> "/"

instance Latexifiable UEOp where
  toLatex = \case
    ESig   -> "sig"
    EAbs   -> "abs"
    EFun x -> toLatex x
    ENot   -> "\\neg"
    ENeg   -> "-"

instance Latexifiable ElemFunction where
  toLatex = \case
    ExtractDay     -> "day"
    ExtractMonth   -> "month"
    ExtractYear    -> "year"
    Prefix a       -> "prefix_{" <> toLatex a <> "}"
    Suffix a       -> "suffix_{" <> toLatex a <> "}"
    SubSeq a b     -> "subseq_{" <> toLatex a <> "," <> toLatex b <> "}"
    AssertLength a -> "assertLenght_{" <> toLatex a <> "}"

instance Latexifiable a => Latexifiable (Expr a) where
  toLatex = \case
    E2 o a b -> spaces [parens $ toLatex a, toLatex o, parens $ toLatex b]
    E1 o a   -> spaces [toLatex o, parens $ toLatex a]
    E0 a     -> toLatex a

instance Latexifiable BPOp where
  toLatex = \case
    PAnd -> "\\land"
    POr  -> "\\lor"

instance Latexifiable UPOp where
  toLatex = \case
    PNot -> "\\neg"

instance Latexifiable x => Latexifiable (Prop x) where
  toLatex = \case
    P2 o a b -> spaces [parens $ toLatex a, toLatex o, parens $ toLatex b]
    P1 o a   -> spaces [toLatex o, parens $ toLatex a]
    P0 a     -> toLatex a

instance Latexifiable BROp where
  toLatex = \case
    REq        -> "="
    RGt        -> ">"
    RLt        -> "<"
    RGe        -> "\\ge"
    RLe        -> "\\le"
    RLike      -> "\\sim"
    RSubstring -> "\\subseteq"

instance Latexifiable a => Latexifiable (Rel a) where
  toLatex = \case
    R2 o a b -> spaces [toLatex a, toLatex o, toLatex b]
    R0 a     -> toLatex a

instance Latexifiable (Latex x) where
  toLatex = coerce

instance Latexifiable e => Latexifiable (BQOp e) where
  toLatex = \case
    QJoin p          -> "\\Join_{" <> toLatex p <> "}"
    QProd            -> "\\times"
    QUnion           -> "\\cup"
    QDistinct        -> "distinct"
    QProjQuery       -> "\\pi_{q}"
    QLeftAntijoin p  -> "\\triangleleft_{" <> toLatex p <> "}"
    QRightAntijoin p -> "\\triangleright_{" <> toLatex p <> "}"

instance Latexifiable e => Latexifiable (Aggr e) where
  toLatex (NAggr f x) = case f of
    AggrSum   -> parens $ "\\sum " <> toLatex x
    AggrCount -> "|" <> toLatex x <> "|"
    AggrAvg   -> "\\mathbf{E}" <> parens (toLatex x)
    AggrMin   -> "min" <> parens (toLatex x)
    AggrMax   -> "max" <> parens (toLatex x)
    AggrFirst -> "first" <> parens (toLatex x)

instance Latexifiable e => Latexifiable (UQOp e) where
  toLatex = \case
    QSel p -> "\\sigma_{" <> toLatex p <> "}"
    QProj p -> "\\pi_{" <> prj p <> "}" where
      prj = comma
            . fmap (\(x,y) -> spaces [toLatex x, "\\mapsto", toLatex y])
    QGroup prj grp -> "a_{" <> comma (toLatex <$> grp') <> "} "
                     <> toLatex (QProj prj') where
      prj' = first toLatex <$> fmap3 toLatex prj
      grp' = fmap2 toLatex grp
    QSort srt -> "s_{" <> comma (toLatex <$> srt) <> "}"
    QLimit l -> "l_{" <> toLatex l <> "}"
    QDrop d -> "d_{" <> toLatex d <> "}"

instance (Latexifiable e, Latexifiable s)  => Latexifiable (Query e s) where
  toLatex = \case
    Q2 o _ _ -> spaces [parens "...", toLatex o, parens "..."]
    Q1 o _   -> spaces [toLatex o, parens "..."]
    Q0 a     -> toLatex a

instance Latexifiable a => Latexifiable (Maybe a) where
  toLatex = \case
    Nothing -> "\\bot"
    Just a  -> toLatex a

simpleRender :: (Latexifiable t,Latexifiable n) => Bipartite t n -> String
simpleRender bip = runRenderM def
  $ toDot'
  $ (`evalState` def{
        gbPropNet=bimap (unLatex . toLatex) (unLatex . toLatex) bip
        })
  $ renderAll def mempty mempty "G"

instance Latexifiable Date where
  toLatex Date{..} = fromString
    $ printf "%d-%d-%d %d:%d:%d"
    year month day hour minute seconds
instance Latexifiable e => Latexifiable (ExpTypeSym' e) where
  toLatex = \case
    EDate x         -> toLatex x
    EInterval x     -> "D(" <> toLatex x <> ")"
    EFloat x        -> fromString $ show x
    EInt x          -> fromString $ show x
    EString x       -> fromString $ "``" ++ x ++ "''"
    ESym x          -> toLatex x
    EBool x         -> fromString $ show x
    ECount Nothing  -> fromString "count(*)"
    ECount (Just e) -> "count(" <> toLatex e <> ")"
