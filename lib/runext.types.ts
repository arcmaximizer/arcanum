// Runtime extensions are Deno code that runs in its own worker in order to do
// things that cannot natively be provided in Arcanum, for example running a
// DNS server.
//
// They are tracked outside of Arcanum and you must be careful when using them
// as they are non-deterministic and therefore a very dangerous potential cause
// of indeterminism.
