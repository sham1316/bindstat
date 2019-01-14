-- MySQL dump 10.13  Distrib 5.1.58, for portbld-freebsd6.2 (i386)
--
-- Host: localhost    Database: statdns
-- ------------------------------------------------------
-- Server version	5.1.58

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `queryes`
--

DROP TABLE IF EXISTS `queryes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `queryes` (
  `Date` int(11) DEFAULT NULL,
  `clientIP` int(4) DEFAULT NULL,
  `View` varchar(25) DEFAULT NULL,
  `query` varchar(255) DEFAULT NULL,
  `type1` varchar(10) DEFAULT NULL,
  `type2` varchar(10) DEFAULT NULL,
  `type3` varchar(10) DEFAULT NULL,
  `serverIP` int(4) DEFAULT NULL,
  `not_parsed_records` varchar(255) DEFAULT NULL,
  KEY `not_parsed_records` (`not_parsed_records`),
  KEY `client_ip_date` (`clientIP`,`Date`),
  KEY `type2` (`type2`),
  KEY `type1` (`type1`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COMMENT='запросы';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2012-04-03 18:55:57
