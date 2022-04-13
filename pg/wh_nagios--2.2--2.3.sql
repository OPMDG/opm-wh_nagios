-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION  wh_nagios" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group


DROP FUNCTION wh_nagios.get_metric_data(text, text, text, timestamp with time zone, timestamp with time zone) ;
