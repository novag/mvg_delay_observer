SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
--
-- Datenbank: `mvg_delay_observer`
--
CREATE DATABASE IF NOT EXISTS `mvg_delay_observer`;
USE `mvg_delay_observer`;

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `departure`
--

CREATE TABLE `departure` (
  `station_id` int(11) NOT NULL,
  `destination_id` int(11) NOT NULL,
  `departure_id` int(11) NOT NULL,
  `departure_time` bigint(20) NOT NULL,
  `product` varchar(16) NOT NULL,
  `label` varchar(64) NOT NULL,
  `live` tinyint(1) NOT NULL,
  `sev` tinyint(1) NOT NULL,
  `lineBackgroundColor` varchar(16) NOT NULL
);

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `schedule`
--

CREATE TABLE `schedule` (
  `station_id` int(11) NOT NULL,
  `mvv_station_id` int(11) NOT NULL,
  `destination_id` int(11) NOT NULL,
  `departure_time` bigint(20) NOT NULL,
  `product` varchar(16) NOT NULL,
  `label` varchar(8) NOT NULL
);

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `line`
--

CREATE TABLE `line` (
  `id` int(11) NOT NULL,
  `divaId` varchar(8) NOT NULL,
  `lineNumber` varchar(32) NOT NULL,
  `product` varchar(16) NOT NULL,
  `partialNet` varchar(32) DEFAULT NULL,
  `sev` tinyint(1) NOT NULL
);

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `station`
--

CREATE TABLE `station` (
  `id` int(11) NOT NULL,
  `station_id` int(11) NOT NULL,
  `type` varchar(32) NOT NULL,
  `name` varchar(64) NOT NULL,
  `aliases` varchar(255) DEFAULT NULL,
  `hasLiveData` tinyint(1) NOT NULL,
  `hasZoomData` tinyint(1) NOT NULL,
  `place` varchar(64) NOT NULL,
  `longitude` float(10,6) DEFAULT NULL,
  `latitude` float(10,6) DEFAULT NULL
);

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `station_product`
--

CREATE TABLE `station_product` (
  `station_id` int(11) NOT NULL,
  `product` varchar(16) NOT NULL
);

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `message`
--

CREATE TABLE `message` (
  `id` int(11) NOT NULL,
  `message_id` int(11) NOT NULL,
  `type` varchar(32) NOT NULL,
  `title` varchar(255) NOT NULL,
  `description` text NOT NULL,
  `publication` bigint(20) NOT NULL,
  `validFrom` bigint(20) NOT NULL,
  `validTo` bigint(20) DEFAULT NULL
);

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `message_line`
--

CREATE TABLE `message_line` (
  `message_id` int(11) NOT NULL,
  `line_id` int(11) NOT NULL,
  `destination_id` int(11) DEFAULT NULL
);

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `transport_device`
--

CREATE TABLE `transport_device` (
  `id` int(11) NOT NULL,
  `station_id` int(11) NOT NULL,
  `type` varchar(32) NOT NULL,
  `identifier` varchar(16) NOT NULL,
  `name` varchar(64) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `status` varchar(32) NOT NULL,
  `oos_since` bigint(20) DEFAULT NULL,
  `oos_until` bigint(20) DEFAULT NULL,
  `oos_description` varchar(255) DEFAULT NULL,
  `timestamp` bigint(20) NOT NULL,
  `xcoordinate` int(11) NOT NULL,
  `ycoordinate` int(11) NOT NULL
);

--
-- Indizes der exportierten Tabellen
--

--
-- Indizes für die Tabelle `departure`
--
ALTER TABLE `departure`
  ADD PRIMARY KEY (`station_id`,`destination_id`,`departure_time`),
  ADD KEY `destination_id` (`destination_id`);

--
-- Indizes für die Tabelle `schedule`
--
ALTER TABLE `schedule`
  ADD PRIMARY KEY (`station_id`,`destination_id`,`departure_time`),
  ADD KEY `destination_id` (`destination_id`);

--
-- Indizes für die Tabelle `line`
--
ALTER TABLE `line`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `lineNumber` (`lineNumber`);

--
-- Indizes für die Tabelle `station`
--
ALTER TABLE `station`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `station_id` (`station_id`);
ALTER TABLE `station` ADD FULLTEXT KEY `name` (`name`);

--
-- Indizes für die Tabelle `station_product`
--
ALTER TABLE `station_product`
  ADD UNIQUE KEY `station_product_key` (`station_id`,`product`) USING BTREE;

--
-- Indizes für die Tabelle `message`
--
ALTER TABLE `message`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `message_id` (`message_id`);

--
-- Indizes für die Tabelle `message_line`
--
ALTER TABLE `message_line`
  ADD UNIQUE KEY `message_line_key` (`message_id`,`line_id`,`destination_id`) USING BTREE;

--
-- Indizes für die Tabelle `transport_device`
--
ALTER TABLE `transport_device`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `search_key` (`station_id`, `xcoordinate`,`ycoordinate`, `timestamp`) USING BTREE;

--
-- AUTO_INCREMENT für exportierte Tabellen
--

--
-- AUTO_INCREMENT für Tabelle `line`
--
ALTER TABLE `line`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT für Tabelle `station`
--
ALTER TABLE `station`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT für Tabelle `message`
--
ALTER TABLE `message`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT für Tabelle `transport_device`
--
ALTER TABLE `transport_device`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- Constraints der exportierten Tabellen
--

--
-- Constraints der Tabelle `departure`
--
ALTER TABLE `departure`
  ADD CONSTRAINT `departure_ibfk_1` FOREIGN KEY (`station_id`) REFERENCES `station` (`station_id`),
  ADD CONSTRAINT `departure_ibfk_2` FOREIGN KEY (`destination_id`) REFERENCES `station` (`station_id`);

--
-- Constraints der Tabelle `schedule`
--
ALTER TABLE `schedule`
  ADD CONSTRAINT `schedule_ibfk_1` FOREIGN KEY (`station_id`) REFERENCES `station` (`station_id`),
  ADD CONSTRAINT `schedule_ibfk_2` FOREIGN KEY (`destination_id`) REFERENCES `station` (`station_id`);

--
-- Constraints der Tabelle `station_product`
--
ALTER TABLE `station_product`
  ADD CONSTRAINT `station_product_ibfk_1` FOREIGN KEY (`station_id`) REFERENCES `station` (`station_id`);

--
-- Constraints der Tabelle `station_product`
--
ALTER TABLE `message_line`
  ADD CONSTRAINT `message_line_ibfk_1` FOREIGN KEY (`message_id`) REFERENCES `message` (`id`),
  ADD CONSTRAINT `message_line_ibfk_2` FOREIGN KEY (`line_id`) REFERENCES `line` (`id`),
  ADD CONSTRAINT `message_line_ibfk_3` FOREIGN KEY (`destination_id`) REFERENCES `station` (`station_id`);

--
-- Constraints der Tabelle `transport_device`
--
ALTER TABLE `transport_device`
  ADD CONSTRAINT `transport_device_ibfk_1` FOREIGN KEY (`station_id`) REFERENCES `station` (`station_id`);
COMMIT;