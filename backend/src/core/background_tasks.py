import asyncio
import logging
from datetime import timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import signal
import sys

from ..database.session import get_session, engine
from ..utils.activity_logger import cleanup_old_activities

logger = logging.getLogger(__name__)

# Global variable to hold the scheduler
_scheduler = None

def start_background_cleanup():
    """Start background scheduler for cleanup tasks"""
    global _scheduler

    try:
        _scheduler = AsyncIOScheduler()

        # Schedule cleanup to run every hour
        _scheduler.add_job(
            func=perform_cleanup,
            trigger=IntervalTrigger(hours=1),
            id='activity_cleanup_job',
            name='Cleanup old activity logs',
            replace_existing=True
        )

        _scheduler.start()
        logger.info("Background cleanup scheduler started")

        # Properly handle shutdown signals to avoid event loop issues
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down scheduler...")
            if _scheduler and _scheduler.running:
                _scheduler.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        return _scheduler
    except ImportError:
        logger.warning("APScheduler not installed. Background cleanup will not be available.")
        return None


async def perform_cleanup():
    """Perform the actual cleanup operation"""
    from sqlmodel import Session

    try:
        # Use the async context manager approach for session
        with Session(engine) as session:
            deleted_count = cleanup_old_activities(session, hours_old=24)
            logger.info(f"Background cleanup completed. Deleted {deleted_count} old activity logs.")
    except Exception as e:
        logger.error(f"Error during background cleanup: {str(e)}")


def stop_background_cleanup():
    """Stop the background scheduler safely"""
    global _scheduler
    try:
        if _scheduler and _scheduler.running:
            _scheduler.shutdown()
            logger.info("Background cleanup scheduler stopped")
    except Exception as e:
        logger.error(f"Error stopping scheduler: {str(e)}")