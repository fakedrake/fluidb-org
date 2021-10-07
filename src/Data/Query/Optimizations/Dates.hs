{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns        #-}
module Data.Query.Optimizations.Dates (squashDates) where

import           Control.Monad.State
import           Data.Query.Algebra
import           Data.Query.Optimizations.Types
import           Data.Query.SQL.Types
import           Data.Time.Calendar
import           Data.Utils.Functors

squashDatesE
  :: forall e' e s .
  SymEmbedding (ExpTypeSym' e) s e'
  -> Expr e'
  -> Maybe (Expr e')
squashDatesE SymEmbedding {..} = qmapM' $ \case
  e@(E2 o el er) -> case (nonSymName el,nonSymName er) of
    (Just (EDate dl),Just (EInterval dr)) -> E0 . embedLit . EDate
      <$> (dl `dateAppend` dr) False
    (Just (EInterval dl),Just (EInterval dr)) -> E0 . embedLit . EDate
      <$> (dl `dateAppend` dr) True
    -- Expect dates to be on the left
    (_,Just (EDate _)) -> err
    (Just (EInterval _),_) -> err
    (Just (EDate _),_) -> err
    (_,_) -> return e
    where
      nonSymName = \case
        E0 e' -> case unEmbed e' of
          ESym _          -> Nothing
          ECount (Just _) -> Nothing
          x               -> Just x
        _ -> Nothing
      err :: Maybe b
      err = Nothing
      dateAppend :: Date -> Date -> Bool -> Maybe Date
      dateAppend dl ((if o == ESub then negDate else id) -> dr) isInterval =
        case  o of
          EAdd -> return $ evalState goAll 0
          ESub -> return $ evalState goAll 0
          _    -> err
        where
          combine f m = do
            carry <- get
            let res = (f dl + f dr) + carry
            if m == 0 then put 0 >> return res else do
              put $ signum res * (abs res `div` m)
              return $ res `mod` m
          goAll :: State Integer Date
          goAll = do
            seconds' <- combine seconds 60
            minute' <- combine minute 60
            hour' <- combine hour 24
            let time =
                  zeroDay { seconds = seconds',minute = minute',hour = hour' }
            dayCarry <- get
            put 0
            month' <- combine month 12
            year' <- combine year 0
            let lday = day dl + dayCarry
            let rday = fromInteger $ day dr
            if isInterval then return
              time { year = year',month = month',day = lday + rday }
              else let (year'',toInteger -> month'',toInteger -> day'') =
                         toGregorian
                         $ rday
                         `addDays` (fromGregorian
                                      year'
                                      (fromInteger month')
                                      (fromInteger lday))
                in return time { year = year'',month = month'',day = day'' }
  e -> return e

squashDates
  :: SymEmbedding (ExpTypeSym' e) s e'
  -> Query e' s
  -> Maybe (Query e' s)
squashDates iso = traverseProp (traverse2 $ squashDatesE iso)
