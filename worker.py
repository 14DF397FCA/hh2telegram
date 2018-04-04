import logging

from telegram.ext import Updater, CommandHandler

from utils.utils import get_telegram_token, JOB_NAME, get_delay_between_messages
from utils.startup import startup
from utils.vacancies import get_new_vacancies

TOKEN = None
CONFIG = None
DELAY_BETWEEN_MESSAGES = None


# Define a few command handlers. These usually take the two arguments bot and
# update. Error handlers also receive the raised TelegramError object in error.
def start(bot, update):
    update.message.reply_text('Use /sched <seconds> to set scheduler.\n'
                              'Use /unsched to remove scheduler task.')


def check_for_vacancies(bot, job):
    new_vacancies = get_new_vacancies(CONFIG)
    for x in new_vacancies:
        bot.send_message(job.context, text=x)


def set_scheduler(bot, update, args, job_queue, chat_data):
    """Add a job to the queue."""
    if JOB_NAME in chat_data:
        update.message.reply_text("Remove exist task")
        return
    chat_id = update.message.chat_id
    try:
        # args[0] should contain the time for the timer in seconds
        due = int(args[0])
        if due < 0:
            update.message.reply_text('Sorry we can not go back to future!')
            return

        # Add job to queue
        job = job_queue.run_repeating(check_for_vacancies, due, context=chat_id)
        chat_data[JOB_NAME] = job

        update.message.reply_text('Timer successfully set!')

    except (IndexError, ValueError):
        update.message.reply_text('Usage: /sched <seconds>')


def list_jobs(bot, update, args, job_queue, chat_data):
    logging.info(chat_data)


def unsched(bot, update, chat_data):
    """Remove the job if the user changed their mind."""
    if JOB_NAME not in chat_data:
        update.message.reply_text('You have no active timer')
        return

    job = chat_data[JOB_NAME]
    job.schedule_removal()
    del chat_data[JOB_NAME]

    update.message.reply_text('Timer successfully unsched!')


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logging.warning('Update "%s" caused error "%s"', update, error)


def main():
    """Run bot."""
    if TOKEN is None:
        logging.error("Telegram token is not specified!")
        return
    updater = Updater(TOKEN)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", start))
    dp.add_handler(CommandHandler("sched", set_scheduler,
                                  pass_args=True,
                                  pass_job_queue=True,
                                  pass_chat_data=True))
    dp.add_handler(CommandHandler("list", list_jobs,
                                  pass_args=True,
                                  pass_job_queue=True,
                                  pass_chat_data=True))
    dp.add_handler(CommandHandler("unsched", unsched, pass_chat_data=True))

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()

    # Block until you press Ctrl-C or the process receives SIGINT, SIGTERM or
    # SIGABRT. This should be used most of the time, since start_polling() is
    # non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    CONFIG = startup()
    TOKEN = get_telegram_token(config=CONFIG)
    DELAY_BETWEEN_MESSAGES = get_delay_between_messages(config=CONFIG)

    main()
