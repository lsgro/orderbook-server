use std::cmp::{min};
use std::ops::Index;
use rust_decimal::prelude::*;
use std::collections::HashMap;

use crate::core::*;


#[derive(PartialEq, Debug)]
enum Ranking {
    LessFirst,
    GreaterFirst
}

#[derive(PartialEq, Debug)]
pub struct AggregateBook {
    bids: AggregateBookSide,
    asks: AggregateBookSide,
}

impl AggregateBook {
    pub fn new(max_levels: usize) -> Self {
        Self {
            bids: AggregateBookSide::new(Ranking::GreaterFirst, max_levels, vec![]),
            asks: AggregateBookSide::new(Ranking::LessFirst, max_levels, vec![]),
        }
    }

    pub fn best_bids(&self) -> Vec<&ExchangeLevel> {
        self.bids.best_levels()
    }

    pub fn best_asks(&self) -> Vec<&ExchangeLevel> {
        self.asks.best_levels()
    }

    pub fn update(&mut self, book_update: BookUpdate) {
        self.bids.update_side(book_update.bids);
        self.asks.update_side(book_update.asks);
    }
}


#[derive(PartialEq, Debug)]
struct AggregateBookSide {
    ordering: Ranking,
    max_levels: usize,
    data: Vec<AggregateLevel>,
}

impl AggregateBookSide {
    fn new(ordering: Ranking, max_levels: usize, data: Vec<AggregateLevel>) -> Self {
        let instance = Self {
            ordering,
            max_levels,
            data,
        };
        instance.check_integrity();
        instance
    }

    fn check_integrity(&self) {
        let mut prev_price: Option<Decimal> = None;
        for level in &self.data {
            if let Some(a_price) = prev_price {
                assert!(
                    self.is_before(a_price, level.price),
                    "Level price {} is not before {}", a_price, level.price
                );
            } else {
                prev_price = Some(level.price);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn best_levels(&self) -> Vec<&ExchangeLevel> {
        let mut result: Vec<&ExchangeLevel> = vec![];
        let mut levels_to_add = self.max_levels;
        if !self.data.is_empty() {
            for price_cons_level in &self.data {
                let price_levels = price_cons_level.levels_by_amount();
                let price_levels_to_add = min(price_levels.len(), levels_to_add);
                result.extend_from_slice(&price_levels[0..price_levels_to_add]);
                levels_to_add -= price_levels_to_add;
                if levels_to_add == 0 {
                    break;
                }
            }
        }
        result
    }

    fn is_before(&self, price_a: Decimal, price_b: Decimal) -> bool {
        match self.ordering {
            Ranking::LessFirst => price_a < price_b,
            Ranking::GreaterFirst => price_a > price_b,
        }
    }

    fn update_side(&mut self, side_update: Vec<ExchangeLevel>) {
        let mut update_strategy = AggregateBookSideUpdateStrategy::new();
        for level_update in side_update {
            if !update_strategy.apply(self, level_update) {
                break;
            }
        }
        self.data.retain(|level| !level.exchange_levels.is_empty());
    }
}

impl Index<usize> for AggregateBookSide {
    type Output = AggregateLevel;

    fn index(&self, rhs: usize) -> &Self::Output {
        &self.data[rhs]
    }
}

struct AggregateBookSideUpdateStrategy {
    current_index: usize,
    prev_update_price: Option<Decimal>,
}

impl AggregateBookSideUpdateStrategy {
    fn new() -> Self {
        Self {
            current_index: 0,
            prev_update_price: None,
        }
    }

    fn apply(&mut self, side: &mut AggregateBookSide, level_update: ExchangeLevel) -> bool {
        // Check that update levels are sorted
        if let Some(a_price) = self.prev_update_price {
            assert!(
                !side.is_before(level_update.price, a_price),
                "Update price {} is before {}", level_update.price, a_price
            );
        }
        self.prev_update_price = Some(level_update.price);

        if self.current_index == side.len() {
            if side.len() >= side.max_levels {
                false
            } else {
                side.data.push(AggregateLevel::from_level(level_update));
                self.current_index += 1;
                true
            }
        } else {
            let price = side[self.current_index].price;
            if side.is_before(level_update.price, price) {
                side.data.insert(self.current_index, AggregateLevel::from_level(level_update));
                self.current_index += 1;
                true
            } else if level_update.price == price {
                side.data[self.current_index].update(level_update);
                self.current_index += 1;
                true
            } else {
                while side.is_before(side[self.current_index].price, level_update.price) {
                    side.data[self.current_index].remove(level_update.exchange);
                    self.current_index += 1;
                    if self.current_index == side.len() {
                        break;
                    }
                }
                self.apply(side, level_update)
            }
        }
    }
}

#[derive(PartialEq, Debug)]
struct AggregateLevel {
    price: Decimal,
    exchange_levels: HashMap<&'static str, ExchangeLevel>,
}

impl AggregateLevel {
    fn from_level(level: ExchangeLevel) -> Self {
        Self {
            price: level.price,
            exchange_levels: HashMap::from([(level.exchange, level)]),
        }
    }

    #[cfg(test)]
    fn from_levels(levels: Vec<ExchangeLevel>) -> Self {
        assert!(!levels.is_empty());
        let mut levels_iter = levels.into_iter();
        let mut cons_level = Self::from_level(levels_iter.next().unwrap());
        for level in levels_iter {
            cons_level.update(level);
        }
        cons_level
    }

    fn update(&mut self, level: ExchangeLevel) {
        assert_eq!(self.price, level.price);
        self.exchange_levels.insert(level.exchange, level);
    }

    fn remove(&mut self, exchange: &'static str) {
        self.exchange_levels.remove(exchange);
    }

    #[cfg(test)]
    fn total_amount(&self) -> Decimal {
        let mut result: Decimal = Decimal::zero();
        for level in self.exchange_levels.values() {
            result += level.amount;
        }
        result
    }

    fn levels_by_amount(&self) -> Vec<&ExchangeLevel> {
        let mut levels: Vec<&ExchangeLevel> = self.exchange_levels.values().collect();
        levels.sort_by(|&a, &b| b.amount.cmp(&a.amount));
        levels
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consolidate_level_create_from_level() {
        let level = ExchangeLevel::from_strs("test", "100.0", "99.9");
        let cons_level = AggregateLevel::from_level(level);
        assert_eq!(cons_level.price, Decimal::from_str("100.0").unwrap());
        assert_eq!(cons_level.total_amount(), Decimal::from_str("99.9").unwrap());
    }

    #[test]
    fn test_consolidate_level_create_from_levels() {
        let level1 = ExchangeLevel::from_strs("test1", "100.0", "99.9");
        let level2 = ExchangeLevel::from_strs("test2", "100.0", "99.9");
        let cons_level = AggregateLevel::from_levels(vec![level1, level2]);
        assert_eq!(cons_level.price, Decimal::from_str("100.0").unwrap());
        assert_eq!(cons_level.total_amount(), Decimal::from_str("199.8").unwrap());
    }

    #[test]
    fn test_consolidate_levels_by_amount() {
        let level1 = ExchangeLevel::from_strs("test1", "100.0", "3");
        let level2 = ExchangeLevel::from_strs("test2", "100.0", "1");
        let level3 = ExchangeLevel::from_strs("test3", "100.0", "2");
        let level4 = ExchangeLevel::from_strs("test4", "100.0", "5");
        let cons_level = AggregateLevel::from_levels(vec![level1, level2, level3, level4]);
        assert_eq!(cons_level.price, Decimal::from_str("100.0").unwrap());
        assert_eq!(cons_level.total_amount(), Decimal::from_str("11").unwrap());
        let levels = cons_level.levels_by_amount();
        assert_eq!(levels[0].amount, Decimal::from_str("5").unwrap());
        assert_eq!(levels[0].exchange, "test4");
        assert_eq!(levels[1].amount, Decimal::from_str("3").unwrap());
        assert_eq!(levels[1].exchange, "test1");
        assert_eq!(levels[2].amount, Decimal::from_str("2").unwrap());
        assert_eq!(levels[2].exchange, "test3");
        assert_eq!(levels[3].amount, Decimal::from_str("1").unwrap());
        assert_eq!(levels[3].exchange, "test2");
    }

    #[test]
    fn test_consolidate_level_create_from_levels_panics_if_different_price() {
        let level1 = ExchangeLevel::from_strs("test1", "100.0", "99.9");
        let level2 = ExchangeLevel::from_strs("test2", "99.0", "99.9");
        let result = std::panic::catch_unwind(|| AggregateLevel::from_levels(vec![level1, level2]));
        assert!(result.is_err());
    }

    #[test]
    fn test_consolidate_level_update_correct() {
        let level1 = ExchangeLevel::from_strs("test1", "100.0", "99.9");
        let level2 = ExchangeLevel::from_strs("test2", "100.0", "90.0");
        let mut cons_level = AggregateLevel::from_level(level1);
        cons_level.update(level2);
        assert_eq!(cons_level.price, Decimal::from_str("100.0").unwrap());
        assert_eq!(cons_level.total_amount(), Decimal::from_str("189.9").unwrap());
    }

    #[test]
    fn test_consolidate_level_update_panics_if_different_price() {
        let level1 = ExchangeLevel::from_strs("test", "100.0", "99.9");
        let level2 = ExchangeLevel::from_strs("test", "99.0", "90.0");
        let mut cons_level = AggregateLevel::from_level(level1);
        let result = std::panic::catch_unwind(move || cons_level.update(level2));
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_book() {
        let mut book = AggregateBook::new(3);
        let book_update = BookUpdate {
            exchange: "test",
            bids: vec![
                ExchangeLevel::from_strs("test", "99", "10"),
                ExchangeLevel::from_strs("test", "98", "10"),
                ExchangeLevel::from_strs("test", "97", "10"),
            ],
            asks: vec![
                ExchangeLevel::from_strs("test", "100", "10"),
                ExchangeLevel::from_strs("test", "101", "10"),
                ExchangeLevel::from_strs("test", "102", "10"),
            ],
        };
        book.update(book_update);
        let exp_book = AggregateBook {
            bids: AggregateBookSide::new(Ranking::GreaterFirst, 3,vec![
                AggregateLevel::from_level(ExchangeLevel::from_strs("test", "99", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test", "98", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test", "97", "10")),
            ]),
            asks: AggregateBookSide::new(Ranking::LessFirst, 3, vec![
                AggregateLevel::from_level(ExchangeLevel::from_strs("test", "100", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test", "101", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test", "102", "10")),
            ]),
        };
        assert_eq!(book, exp_book);
    }

    #[test]
    fn test_insert_into_bids_side() {
        let mut bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
        ]);
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "100", "10"),
            ExchangeLevel::from_strs("test2", "98", "10"),
            ExchangeLevel::from_strs("test2", "94", "10"),
        ];
        bids.update_side(bids_update);
        let exp_bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "100", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "98", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "94", "10")),
        ]);
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_add_at_beginning_to_bids_side() {
        let mut bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
        ]);
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "102", "10"),
            ExchangeLevel::from_strs("test2", "101", "10"),
            ExchangeLevel::from_strs("test2", "100", "10"),
        ];
        bids.update_side(bids_update);
        let exp_bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "101", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "100", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
        ]);
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_add_at_end_to_bids_side() {
        let mut bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
        ]);
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "94", "10"),
            ExchangeLevel::from_strs("test2", "93", "10"),
            ExchangeLevel::from_strs("test2", "92", "10"),
        ];
        bids.update_side(bids_update);
        let exp_bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "94", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "93", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "92", "10")),
        ]);
        assert_eq!(bids, exp_bids);
    }

    #[test]
    fn test_insert_into_asks_side() {
        let mut asks = AggregateBookSide::new(Ranking::LessFirst,10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ]);
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "103", "10"),
            ExchangeLevel::from_strs("test2", "105", "10"),
            ExchangeLevel::from_strs("test2", "107", "10"),
        ];
        asks.update_side(asks_update);
        let exp_asks = AggregateBookSide::new(Ranking::LessFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "103", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "105", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "107", "10")),
        ]);
        assert_eq!(asks, exp_asks);
    }

    #[test]
    fn test_add_at_beginning_to_asks_side() {
        let mut asks = AggregateBookSide::new(Ranking::LessFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ]);
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "99", "10"),
            ExchangeLevel::from_strs("test2", "100", "10"),
            ExchangeLevel::from_strs("test2", "101", "10"),
        ];
        asks.update_side(asks_update);
        let exp_asks = AggregateBookSide::new(Ranking::LessFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "100", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "101", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ]);
        assert_eq!(asks, exp_asks);
    }

    #[test]
    fn test_add_at_end_to_asks_side() {
        let mut asks = AggregateBookSide::new(Ranking::LessFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ]);
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "107", "10"),
            ExchangeLevel::from_strs("test2", "108", "10"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        asks.update_side(asks_update);
        let exp_asks = AggregateBookSide::new(Ranking::LessFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "107", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "108", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "109", "10")),
        ]);
        assert_eq!(asks, exp_asks);
    }

    #[test]
    fn test_update_and_add_into_bids_side() {
        let mut bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "98", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
        ]);
        let bids_update = vec![
            ExchangeLevel::from_strs("test1", "99", "5"),
            ExchangeLevel::from_strs("test1", "98", "15"),
            ExchangeLevel::from_strs("test2", "96", "10"),
        ];
        bids.update_side(bids_update);
        let level1 = &bids[0];
        assert_eq!(level1.price, Decimal::from_str("99").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("5").unwrap());
        let level2 = &bids[1];
        assert_eq!(level2.price, Decimal::from_str("98").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &bids[2];
        assert_eq!(level3.price, Decimal::from_str("97").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &bids[3];
        assert_eq!(level4.price, Decimal::from_str("96").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_merge_update_and_add_into_bids_side() {
        let mut bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "98", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
        ]);
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "99", "5"),
            ExchangeLevel::from_strs("test1", "98", "15"),
            ExchangeLevel::from_strs("test2", "96", "10"),
        ];
        bids.update_side(bids_update);
        let level1 = &bids[0];
        assert_eq!(level1.price, Decimal::from_str("99").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("15").unwrap());
        let level2 = &bids[1];
        assert_eq!(level2.price, Decimal::from_str("98").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &bids[2];
        assert_eq!(level3.price, Decimal::from_str("97").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &bids[3];
        assert_eq!(level4.price, Decimal::from_str("96").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_merge_and_add_into_bids_side() {
        let mut bids = AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "98", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
        ]);
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "99", "10"),
            ExchangeLevel::from_strs("test2", "98", "10"),
            ExchangeLevel::from_strs("test2", "96", "10"),
        ];
        bids.update_side(bids_update);
        let level1 = &bids[0];
        assert_eq!(level1.price, Decimal::from_str("99").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("20").unwrap());
        let level2 = &bids[1];
        assert_eq!(level2.price, Decimal::from_str("98").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("20").unwrap());
        let level3 = &bids[2];
        assert_eq!(level3.price, Decimal::from_str("97").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &bids[3];
        assert_eq!(level4.price, Decimal::from_str("96").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_merge_and_add_into_asks_side() {
        let mut asks = AggregateBookSide::new(Ranking::LessFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ]);
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "102", "10"),
            ExchangeLevel::from_strs("test2", "104", "10"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        asks.update_side(asks_update);
        let level1 = &asks[0];
        assert_eq!(level1.price, Decimal::from_str("102").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("20").unwrap());
        let level2 = &asks[1];
        assert_eq!(level2.price, Decimal::from_str("104").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("20").unwrap());
        let level3 = &asks[2];
        assert_eq!(level3.price, Decimal::from_str("106").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &asks[3];
        assert_eq!(level4.price, Decimal::from_str("109").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_update_and_add_into_asks_side() {
        let mut asks = AggregateBookSide::new(Ranking::LessFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ]);
        let asks_update = vec![
            ExchangeLevel::from_strs("test1", "102", "5"),
            ExchangeLevel::from_strs("test1", "104", "15"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        asks.update_side(asks_update);
        let level1 = &asks[0];
        assert_eq!(level1.price, Decimal::from_str("102").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("5").unwrap());
        let level2 = &asks[1];
        assert_eq!(level2.price, Decimal::from_str("104").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &asks[2];
        assert_eq!(level3.price, Decimal::from_str("106").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &asks[3];
        assert_eq!(level4.price, Decimal::from_str("109").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_merge_update_and_add_into_asks_side() {
        let mut asks = AggregateBookSide::new(Ranking::LessFirst, 10, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ]);
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "102", "5"),
            ExchangeLevel::from_strs("test1", "104", "15"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        asks.update_side(asks_update);
        let level1 = &asks[0];
        assert_eq!(level1.price, Decimal::from_str("102").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("15").unwrap());
        let level2 = &asks[1];
        assert_eq!(level2.price, Decimal::from_str("104").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &asks[2];
        assert_eq!(level3.price, Decimal::from_str("106").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
        let level4 = &asks[3];
        assert_eq!(level4.price, Decimal::from_str("109").unwrap());
        assert_eq!(level4.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_update_into_bids_side_with_trimming() {
        let mut bids = AggregateBookSide::new(Ranking::GreaterFirst, 3, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "98", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "97", "10")),
        ]);
        let bids_update = vec![
            ExchangeLevel::from_strs("test2", "99", "5"),
            ExchangeLevel::from_strs("test1", "98", "15"),
            ExchangeLevel::from_strs("test2", "96", "10"),
        ];
        bids.update_side(bids_update);
        assert_eq!(bids.len(), 3);
        let level1 = &bids[0];
        assert_eq!(level1.price, Decimal::from_str("99").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("15").unwrap());
        let level2 = &bids[1];
        assert_eq!(level2.price, Decimal::from_str("98").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &bids[2];
        assert_eq!(level3.price, Decimal::from_str("97").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_update_into_asks_side_with_trimming() {
        let mut asks = AggregateBookSide::new(Ranking::LessFirst, 3, vec![
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
            AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "106", "10")),
        ]);
        let asks_update = vec![
            ExchangeLevel::from_strs("test2", "102", "5"),
            ExchangeLevel::from_strs("test1", "104", "15"),
            ExchangeLevel::from_strs("test2", "109", "10"),
        ];
        asks.update_side(asks_update);
        assert_eq!(asks.len(), 3);
        let level1 = &asks[0];
        assert_eq!(level1.price, Decimal::from_str("102").unwrap());
        assert_eq!(level1.total_amount(), Decimal::from_str("15").unwrap());
        let level2 = &asks[1];
        assert_eq!(level2.price, Decimal::from_str("104").unwrap());
        assert_eq!(level2.total_amount(), Decimal::from_str("15").unwrap());
        let level3 = &asks[2];
        assert_eq!(level3.price, Decimal::from_str("106").unwrap());
        assert_eq!(level3.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_book_amounts() {
        let mut book = AggregateBook {
            bids: AggregateBookSide::new(Ranking::GreaterFirst, 10,vec![
                AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "97", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
            ]),
            asks: AggregateBookSide::new(Ranking::LessFirst, 10, vec![
                AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "106", "10")),
            ]),
        };
        let book_update1 = BookUpdate {
            exchange: "test1",
            bids: vec![
                ExchangeLevel::from_strs("test1", "100", "10"),
                ExchangeLevel::from_strs("test1", "99", "10"),
                ExchangeLevel::from_strs("test1", "97", "5"),
                ExchangeLevel::from_strs("test1", "95", "5"),
            ],
            asks: vec![
                ExchangeLevel::from_strs("test1", "102", "10"),
                ExchangeLevel::from_strs("test1", "103", "10"),
                ExchangeLevel::from_strs("test1", "104", "10"),
                ExchangeLevel::from_strs("test1", "105", "10"),
                ExchangeLevel::from_strs("test1", "106", "5"),
            ],
        };
        book.update(book_update1);
        let book_update2 = BookUpdate {
            exchange: "test2",
            bids: vec![
                ExchangeLevel::from_strs("test2", "100", "20"),
                ExchangeLevel::from_strs("test2", "97", "15"),
                ExchangeLevel::from_strs("test2", "94", "10"),
            ],
            asks: vec![
                ExchangeLevel::from_strs("test2", "102", "10"),
                ExchangeLevel::from_strs("test2", "105", "10"),
                ExchangeLevel::from_strs("test2", "106", "10"),
                ExchangeLevel::from_strs("test2", "107", "10"),
            ],
        };
        book.update(book_update2);

        assert_eq!(book.bids.len(), 5);
        let bid1 = &book.bids[0];
        assert_eq!(bid1.price, Decimal::from_str("100").unwrap());
        assert_eq!(bid1.total_amount(), Decimal::from_str("30").unwrap());
        let bid2 = &book.bids[1];
        assert_eq!(bid2.price, Decimal::from_str("99").unwrap());
        assert_eq!(bid2.total_amount(), Decimal::from_str("10").unwrap());
        let bid3 = &book.bids[2];
        assert_eq!(bid3.price, Decimal::from_str("97").unwrap());
        assert_eq!(bid3.total_amount(), Decimal::from_str("20").unwrap());
        let bid4 = &book.bids[3];
        assert_eq!(bid4.price, Decimal::from_str("95").unwrap());
        assert_eq!(bid4.total_amount(), Decimal::from_str("5").unwrap());
        let bid5 = &book.bids[4];
        assert_eq!(bid5.price, Decimal::from_str("94").unwrap());
        assert_eq!(bid5.total_amount(), Decimal::from_str("10").unwrap());

        assert_eq!(book.asks.len(), 6);
        let ask1 = &book.asks[0];
        assert_eq!(ask1.price, Decimal::from_str("102").unwrap());
        assert_eq!(ask1.total_amount(), Decimal::from_str("20").unwrap());
        let ask2 = &book.asks[1];
        assert_eq!(ask2.price, Decimal::from_str("103").unwrap());
        assert_eq!(ask2.total_amount(), Decimal::from_str("10").unwrap());
        let ask3 = &book.asks[2];
        assert_eq!(ask3.price, Decimal::from_str("104").unwrap());
        assert_eq!(ask3.total_amount(), Decimal::from_str("10").unwrap());
        let ask4 = &book.asks[3];
        assert_eq!(ask4.price, Decimal::from_str("105").unwrap());
        assert_eq!(ask4.total_amount(), Decimal::from_str("20").unwrap());
        let ask5 = &book.asks[4];
        assert_eq!(ask5.price, Decimal::from_str("106").unwrap());
        assert_eq!(ask5.total_amount(), Decimal::from_str("15").unwrap());
        let ask6 = &book.asks[5];
        assert_eq!(ask6.price, Decimal::from_str("107").unwrap());
        assert_eq!(ask6.total_amount(), Decimal::from_str("10").unwrap());
    }

    #[test]
    fn test_book_update_panics_if_wrong_order() {
        let mut book = AggregateBook {
            bids: AggregateBookSide::new(Ranking::GreaterFirst, 10, vec![
                AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "99", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "97", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "95", "10")),
            ]),
            asks: AggregateBookSide::new(Ranking::LessFirst, 10, vec![
                AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "102", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test1", "104", "10")),
                AggregateLevel::from_level(ExchangeLevel::from_strs("test2", "106", "10")),
            ]),
        };
        let book_update = BookUpdate {
            exchange: "test1",
            bids: vec![
                ExchangeLevel::from_strs("test1", "99", "10"), // <- wrong order
                ExchangeLevel::from_strs("test1", "100", "10"),
            ],
            asks: vec![
                ExchangeLevel::from_strs("test1", "102", "10"),
                ExchangeLevel::from_strs("test1", "103", "10"),
            ],
        };
        let result = std::panic::catch_unwind(move || book.update(book_update));
        assert!(result.is_err());
    }

    #[test]
    fn test_book_best_bids() {
        let book = AggregateBook {
            bids: AggregateBookSide::new(Ranking::GreaterFirst, 3, vec![
                AggregateLevel::from_levels(vec![
                    ExchangeLevel::from_strs("test1", "101", "5"),
                    ExchangeLevel::from_strs("test2", "101", "10"),
                ]),
                AggregateLevel::from_levels(vec![
                    ExchangeLevel::from_strs("test2", "100", "10")
                ]),
                AggregateLevel::from_levels(vec![
                    ExchangeLevel::from_strs("test1", "99", "10")
                ]),
            ]),
            asks: AggregateBookSide::new(Ranking::LessFirst, 3, vec![]),
        };
        let best_bids = book.best_bids();
        assert_eq!(best_bids, vec![
            &ExchangeLevel::from_strs("test2", "101", "10"),
            &ExchangeLevel::from_strs("test1", "101", "5"),
            &ExchangeLevel::from_strs("test2", "100", "10"),
        ]);
    }

    #[test]
    fn test_book_best_asks() {
        let book = AggregateBook {
            bids: AggregateBookSide::new(Ranking::GreaterFirst, 3, vec![]),
            asks: AggregateBookSide::new(Ranking::LessFirst, 3, vec![
                AggregateLevel::from_levels(vec![
                    ExchangeLevel::from_strs("test1", "99", "5"),
                    ExchangeLevel::from_strs("test2", "99", "10"),
                    ExchangeLevel::from_strs("test3", "99", "2"),
                ]),
                AggregateLevel::from_levels(vec![
                    ExchangeLevel::from_strs("test2", "100", "10")
                ]),
                AggregateLevel::from_levels(vec![
                    ExchangeLevel::from_strs("test1", "101", "10")
                ]),
            ]),
        };
        let best_asks = book.best_asks();
        assert_eq!(best_asks, vec![
            &ExchangeLevel::from_strs("test2", "99", "10"),
            &ExchangeLevel::from_strs("test1", "99", "5"),
            &ExchangeLevel::from_strs("test3", "99", "2"),
        ]);
    }
}