<?php

namespace Cgonser\SwiftMailerDatabaseS3SpoolBundle\Command;

use Cgonser\SwiftMailerDatabaseS3SpoolBundle\Spool\DatabaseS3Spool;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class SendQueuedMessagesCommand extends Command
{
    protected static $defaultName = 'cgonser:mailer:send';

    /** @var DatabaseS3Spool */
    private $databaseS3Spool;

    public function __construct(DatabaseS3Spool $databaseS3Spool)
    {
        parent::__construct();
        $this->databaseS3Spool = $databaseS3Spool;
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Send queued messages using the database/S3 spool transport.')
            ->addOption('message_limit', null, InputOption::VALUE_OPTIONAL, 'Messages limit', 10)
            ->addOption('time_limit', null, InputOption::VALUE_OPTIONAL, 'Time limit in seconds', null);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $messageLimit = (int) $input->getOption('message_limit');
        $timeLimit = $input->getOption('time_limit');
        $timeLimit = $timeLimit !== null ? (int) $timeLimit : null;

        $this->databaseS3Spool
            ->setMessageLimit($messageLimit)
            ->setTimeLimit($timeLimit);

        $sentCount = $this->databaseS3Spool->flushQueue();

        $output->writeln(sprintf('Sent %d queued message(s).', $sentCount));

        return Command::SUCCESS;
    }
}
