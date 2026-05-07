# Swift Mailer Database S3 Spool (Symfony Mailer migration)

A Symfony bundle that queues messages in a database and stores message payloads in an Amazon S3 bucket, now using **Symfony Mailer** APIs.

It requires the [AWS PHP SDK](https://github.com/aws/aws-sdk-php) and relies on Doctrine for data persistence.

## Installation

Install via Composer:

```json
{
    "require": {
        "cgonser/swiftmailer-database-s3-spool-bundle": "dev-master"
    }
}
```

Then enable the bundle (if your Symfony version does not auto-register bundles):

```php
// config/bundles.php
return [
    // ...
    Cgonser\SwiftMailerDatabaseS3SpoolBundle\CgonserSwiftMailerDatabaseS3SpoolBundle::class => ['all' => true],
];
```

## Configuration

Configure the bundle in your app config:

```yaml
# config/packages/cgonser_swift_mailer_database_s3_spool.yaml
cgonser_swift_mailer_database_s3_spool:
    s3:
        bucket: '<TARGET BUCKET>'
        region: '<S3 REGION>'
        folder: '<TARGET FOLDER>' # optional
```

Optional explicit AWS credentials:

```yaml
cgonser_swift_mailer_database_s3_spool:
    s3:
        bucket: '<TARGET BUCKET>'
        region: '<S3 REGION>'
        key: '<YOUR AWS KEY>'
        secret: '<YOUR AWS SECRET>'
```

Import bundle services and wire Symfony Mailer to the DB/S3 transport service:

```yaml
# config/services.yaml
imports:
    - { resource: '@CgonserSwiftMailerDatabaseS3SpoolBundle/Resources/config/services.yml' }

services:
    mailer.transport:
        alias: mailer.transport.db_s3
```

Configure framework mailer (required by Symfony Mailer):

```yaml
# config/packages/mailer.yaml
framework:
    mailer:
        dsn: 'null://null'
```

## Sending queued messages

Messages are queued when `MailerInterface::send()` is called through this transport. Process the queue with:

```console
php bin/console cgonser:mailer:send
```

Useful options:

```console
php bin/console cgonser:mailer:send --message_limit=100 --time_limit=60
```

## Mail Queue Entity

By default, the mail queue is stored in `cgonser_mail_queue`, but you can override the entity class using:

```yaml
cgonser_swift_mailer_database_s3_spool:
    entity_class: '<YOUR NEW ENTITY>' # e.g. App\Entity\MailQueue
```

After setup, update your database schema:

```console
php bin/console doctrine:schema:update --force
```

Keep in mind that this bundle relies on the default entity structure and changing it may break behavior.
