// Package errs 定義跨服務共用的錯誤分類。
package errs

import "errors"

// ErrPermanent 標記「確定的業務拒絕」：例如點數不足、庫存不足、請求已作廢。
// 這類錯誤代表操作「確定沒有執行」，重試也不會成功，所以 Saga 不重試、
// 也不需要補償這一步。
//
// 沒有包 ErrPermanent 的錯誤一律視為「暫時性 / 結果不確定」
// （網路逾時、連線中斷…），Saga 會重試，最後仍失敗時，
// 會把這一步也納入補償範圍——因為它「可能」其實已經執行成功了。
var ErrPermanent = errors.New("permanent")

func IsPermanent(err error) bool { return errors.Is(err, ErrPermanent) }
