"""╔══════════════════════════════════════════════════════════════════════════════╗║   ENERGY DETECTOR — WEBHOOK SERVER                                          ║║   Receives TradingView alerts, logs signals, sends Telegram notifications   ║║   Tracks D+1 performance and generates weekly optimization reports          ║║   Designed by Manus AI — Deploy on Render.com (free tier)                  ║╚══════════════════════════════════════════════════════════════════════════════╝"""

import osimport jsonimport sqlite3import loggingimport asyncioimport httpximport yfinance as yffrom datetime import datetime, timedeltafrom fastapi import FastAPI, Request, HTTPException, BackgroundTasksfrom fastapi.responses import JSONResponse, HTMLResponsefrom apscheduler.schedulers.asyncio import AsyncIOSchedulerfrom apscheduler.triggers.cron import CronTrigger

────
