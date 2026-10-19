-- =====================================================================
-- 點數服務（Points Service）
-- =====================================================================
CREATE TABLE IF NOT EXISTS user_points (
    user_id    BIGINT      NOT NULL PRIMARY KEY,
    balance    BIGINT      NOT NULL,
    updated_at DATETIME(3) NOT NULL
) ENGINE = InnoDB;

-- 點數流水表（Ledger）：每一筆點數異動都留下一列，只新增、不修改、不刪除。
--   - request_id 唯一鍵：冪等性的「最終防線」
--   - saga_request_id ：這筆異動屬於哪一次兌換，對帳 / 排程查證時用它把正向跟補償串起來
--   - balance_before / balance_after：任何一列單獨拿出來都能驗算（before + delta = after）
--   - biz_type：
--       redeem_deduct  兌換扣點
--       redeem_refund  兌換失敗退點（Saga 補償）
--       admin_adjust   管理員調整 / 儲值
--       void           作廢標記：補償時發現正向請求「沒有生效」，用正向 request_id 佔位，
--                      擋掉之後才晚到的正向請求（防「懸掛」）。delta 固定為 0，不影響餘額
CREATE TABLE IF NOT EXISTS points_ledger (
    id              BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    request_id      VARCHAR(128) NOT NULL,
    saga_request_id VARCHAR(128) NULL,
    user_id         BIGINT       NOT NULL,
    biz_type        VARCHAR(32)  NOT NULL,
    delta           BIGINT       NOT NULL,
    balance_before  BIGINT       NOT NULL,
    balance_after   BIGINT       NOT NULL,
    created_at      DATETIME(3)  NOT NULL,
    UNIQUE KEY uk_request_id (request_id),
    KEY idx_saga (saga_request_id),
    KEY idx_user_time (user_id, created_at)
) ENGINE = InnoDB;

-- =====================================================================
-- 庫存服務（Inventory Service）
-- =====================================================================
CREATE TABLE IF NOT EXISTS inventory (
    item_id    BIGINT      NOT NULL PRIMARY KEY,
    stock      BIGINT      NOT NULL,
    updated_at DATETIME(3) NOT NULL
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS inventory_transactions (
    id          BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    request_id  VARCHAR(128) NOT NULL,
    item_id     BIGINT       NOT NULL,
    delta       BIGINT       NOT NULL,
    stock_after BIGINT       NOT NULL,
    kind        VARCHAR(16)  NOT NULL,
    created_at  DATETIME(3)  NOT NULL,
    UNIQUE KEY uk_request_id (request_id)
) ENGINE = InnoDB;

-- =====================================================================
-- 道具服務（Item Service）
-- status = granted / revoked（已被補償收回）/ void（作廢標記）
-- =====================================================================
CREATE TABLE IF NOT EXISTS user_items (
    id         BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    request_id VARCHAR(128) NOT NULL,
    user_id    BIGINT       NOT NULL,
    item_id    BIGINT       NOT NULL,
    status     VARCHAR(16)  NOT NULL,
    created_at DATETIME(3)  NOT NULL,
    updated_at DATETIME(3)  NOT NULL,
    UNIQUE KEY uk_request_id (request_id),
    KEY idx_user (user_id)
) ENGINE = InnoDB;

-- =====================================================================
-- Saga 協調器的狀態表：把「流程走到哪」持久化，程式崩潰重啟後才接得回來
-- =====================================================================
CREATE TABLE IF NOT EXISTS sagas (
    id               BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    request_id       VARCHAR(128) NOT NULL,
    user_id          BIGINT       NOT NULL,
    item_id          BIGINT       NOT NULL,
    cost             BIGINT       NOT NULL,
    status           VARCHAR(32)  NOT NULL, -- pending / completed / failed / compensating / compensated / needs_manual
    current_step     INT          NOT NULL, -- 已「確認成功」的正向步驟數
    comp_next        INT          NOT NULL, -- 下一個要補償的步驟（1-based），0 代表補償完成
    comp_retry_count INT          NOT NULL,
    first_failed_at  DATETIME(3)  NULL,
    next_attempt_at  DATETIME(3)  NULL,
    last_error       TEXT         NULL,
    lease_owner      VARCHAR(64)  NULL,     -- 租約：避免多個 worker / 請求同時推進同一個 saga
    lease_until      DATETIME(3)  NULL,
    created_at       DATETIME(3)  NOT NULL,
    updated_at       DATETIME(3)  NOT NULL,
    UNIQUE KEY uk_request_id (request_id),
    KEY idx_status_next (status, next_attempt_at)
) ENGINE = InnoDB;
