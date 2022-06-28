(async () => {
  require('dotenv').config();
  const Graphviz = require('graphviz');
  const Fs = require('fs').promises;
  const Util = require('util');
  const Sharp = require('sharp');
  const fetch = require('node-fetch');
  const Yauzl = require('yauzl-promise');
  const Discord = require('discord.js');
  const IntentFlag = Discord.Intents.FLAGS;
  const DiscordClient = new Discord.Client({
    intents:
      IntentFlag.GUILDS |
      //IntentFlag.GUILD_MEMBERS |
      IntentFlag.GUILD_MESSAGES |
      IntentFlag.GUILD_MESSAGE_REACTIONS,
    makeCache: Discord.Options.cacheWithLimits({
      MessageManager: {
        sweepInterval: 2111,
        sweepFilter: Discord.Sweepers.filterByLifetime({
          lifetime: 31 * 60,
          getComparisonTimestamp: (e) => e.editedTimestamp ?? e.createdTimestamp,
        }),
      },
    }),
    disableMentions: 'everyone',
  });
  const Mongoose = require('mongoose'); //Mongoose.set('debug', true) //////////////////////////
  require('mongoose-long')(Mongoose);
  const Database = await Mongoose.connect('mongodb://127.0.0.1/relationships', {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    useCreateIndex: true,
  });

  var PremiumGuilds = new Map(Object.entries(require('./premium.json')));
  var Statistics = require('./statistics.json');

  const SchemaTypes = Mongoose.Schema.Types;
  const Types = Mongoose.Types;
  const RelationSchema = new Mongoose.Schema(
    {
      id: { type: SchemaTypes.Long, index: true },
      t: SchemaTypes.Mixed, //TODO check if custom type being a number causes problems
    },
    { _id: false }
  );

  const UserSchema = new Mongoose.Schema(
    {
      _id: SchemaTypes.Long,
      fam: [RelationSchema],
      bloc: { type: [SchemaTypes.Long], index: true },
      pass: String,
    },
    { versionKey: false }
  ); //Mongoose functionality limited by this being disabled (save)

  const User = Mongoose.model('User', UserSchema);

  const Prefix = 'r:';
  const RelationshipLimit = 20;
  const PremiumRelationshipLimit = 50;
  const RequestTimeout = 30 * 60 * 1000;
  const ReactionDelay = 2000;
  const YesEmoji = '✅';
  const NoEmoji = '❌';
  const ConfirmEmoji = '💜';
  const ConfirmRemoveEmoji = '✔️';
  const StatisticsWeight = 10;
  const StatisticsSaveInterval = 4 * 60 * 1000;
  const FreeRenderSlots = 5;
  const PremiumRenderSlots = 5;
  const CdnDownloadSlots = 20;
  const RateMeasureTime = 30 * 1000;
  const RateThresholdWeight = 10;
  const RatelimitTime = 5 * 60 * 1000;

  const PrefixLength = Prefix.length;

  var ErrorChannel;
  var LogError = function () {
    const defaultLog = () => console.error.apply(console.error, arguments);

    if (ErrorChannel != null) ErrorChannel.send('```\n' + Util.inspect(arguments.length > 1 ? arguments : arguments[0]).substr(0, 1992) + '\n```').catch(defaultLog);
    else defaultLog();
  };

  var Me;
  var Shard;
  var ShardIdPrefix;
  DiscordClient.on('ready', async () => {
    Me = DiscordClient.user;
    console.log(`Logged in as ${Me.tag}!`);
    Shard = DiscordClient.shard;
    ShardIdPrefix = `${Shard.ids[0].toString(32)}:`;
    if (process.env.ERRORCHANNEL) {
      try {
        ErrorChannel = await DiscordClient.channels.fetch(process.env.ERRORCHANNEL);
      } catch (err) {
        LogError(err);
      }
    }
    DiscordClient.on('error', LogError);
  });

  var Helps = new Map();
  var Images = { anipics: [], animap: new Map() };

  async function LoadHelp() {
    Helps = new Map(Object.entries(JSON.parse(await Fs.readFile('help.json', 'utf8'))).map(([comm, help]) => [comm, help.replace(/\$PREFIX/g, Prefix)]));
  }
  LoadHelp();
  const ReloadHelpCommand = async (message) => {
    if (!IsDeveloper(message)) return;
    await LoadHelp();
    React(message, ConfirmEmoji).catch(LogError);
  };
  const ReloadPremiumCommand = async (message) => {
    if (!IsDeveloper(message)) return;
    PremiumGuilds = new Map(Object.entries(JSON.parse(await Fs.readFile('premium.json', 'utf8'))));
    React(message, ConfirmEmoji).catch(LogError);
  };
  const AddPremiumCommand = async (message, command, commandBase) => {
    if (!IsDeveloper(message)) return;

    command = command.substr(commandBase.length + 1).trim();
    if (command.length === 0) return;

    let guildId = command;

    PremiumGuilds.set(guildId, 1);
    Shard.broadcastEval((client, { guildId }) => client.PremiumGuilds.set(guildId, 1), { context: { guildId } });
    try {
      await Fs.writeFile('premium.json', JSON.stringify(Object.fromEntries(PremiumGuilds), null, '\t'));
      React(message, ConfirmEmoji);
    } catch (err) {
      LogError(err);
    }
  };
  const RemovePremiumCommand = async (message, command, commandBase) => {
    if (!IsDeveloper(message)) return;

    command = command.substr(commandBase.length + 1).trim();
    if (command.length === 0) return;

    let guildId = command;

    PremiumGuilds.delete(guildId);
    Shard.broadcastEval((client, { guildId }) => client.PremiumGuilds.delete(guildId), { context: { guildId } });
    try {
      await Fs.writeFile('premium.json', JSON.stringify(Object.fromEntries(PremiumGuilds), null, '\t'));
      React(message, ConfirmRemoveEmoji);
    } catch (err) {
      LogError(err);
    }
  };
  const ListPremiumCommand = async (message) => {
    if (!IsDeveloper(message)) return;

    Reply(message, [...PremiumGuilds.keys()].join('\n')).catch(LogError);
  };
  async function LoadImages() {
    Images.anipics = (await Fs.readdir('pics/anipics/metadata')).map((x) => x.substr(0, x.length - 5)); //remove .json
    Images.animap = new Map(Images.anipics.map((name, i) => [name.toLowerCase(), i]));
    //Images.anipics.sort();
  }
  LoadImages();
  const ReloadImagesCommand = async (message) => {
    if (!IsDeveloper(message)) return;
    await LoadImages();
    React(message, ConfirmEmoji).catch(LogError);
  };

  const SetPresenceCommand = async (message, command) => {
    if (!IsDeveloper(message)) return;
    try {
      let presence = JSON.parse(/{.*}/s.exec(command)[0]);
      Shard.broadcastEval((client, { presence }) => client.user.setPresence(presence).catch(client.LogError), { context: { presence } });
    } catch (err) {
      LogError(err);
    }
  };

  const SetErrorChannelCommand = async (message) => {
    if (!IsDeveloper(message)) return;
    ErrorChannel = message.channel;
    process.env.ERRORCHANNEL = ErrorChannel.id;
  };

  const GarbageCollectCommand = (message) => {
    if (!IsDeveloper(message)) return;
    if (global.gc) {
      global.gc();
      React(message, ConfirmEmoji).catch(LogError);
    } else Reply(message, "GC isn't exposed!").catch(LogError);
  };

  const MemoryCommand = async (message) => {
    if (!IsDeveloper(message)) return;
    let usages = await Shard.broadcastEval(() => process.memoryUsage());
    let usage = Object.keys(usages[0]).map((key) => [key, usages.reduce((acc, o) => acc + o[key], 0)]);
    let msg = usage.map(([type, size]) => `${type} ${Math.round(size / 1048576)} MB`).join('\n');
    Reply(message, msg).catch(LogError);
  };

  const HelpCommand = async (message, command, commandBase) => {
    try {
      if (command.length !== commandBase.length) {
        let help = Helps.get(command.substr(commandBase.length + 1).trim());
        if (help !== undefined) return await Reply(message, help);
      }
      await Reply(message, Helps.get('help'));
    } catch (err) {
      LogError(err);
    }
  };

  const RelationshipIndex = [
    'unknown', //needed for example for typeIdx || type
    'waifu',
    'husbando',
    'bro',
    'sis',
    'brother',
    'sister',
    'grandpa',
    'grandma',
    'oniichan',
    'oniisan',
    'oneechan',
    'oneesan',
    'friend',
    'classmate',
    'master',
    'mistress',
    'owner',
    'pet',
    'slave',
    'homie',
    'uncle',
    'auntie',
    'son',
    'daughter',
    'child',
    'niece',
    'nephew',
    'cousin',
    'grandson',
    'granddaughter',
    'grandchild',
    'grandparent',
    'parent',
    'partner',
    'little',
    'little one',
    'kid',
    'love',
    'lover',
    'companion',
    'daddy',
    'mommy',
    'little girl',
    'little boy',
    'papa',
    'mama',
    'dom',
    'sub',
    'hypnotist',
    'subject',
    'caretaker',
    'caregiver',
    'mate',
    'husband',
    'wife',
    'dad',
    'mom',
    'cuddlebuddy',
  ];
  const RelationshipColors = {
    '#4876ff': 'dad, papa, daddy, grandpa, uncle',
    '#ee3b3b': 'mom, mama, mommy, grandma, auntie',
    '#ff34b3': 'waifu, wife, mistress',
    '#ab82ff': 'husbando, husband, master',
    '#b3ee3a': 'bro, brother, oniichan, oniisan',
    '#ffa500': 'sis, sister, oneechan, oneesan',
    '#00e5ee': 'son, nephew, granddaughter, little boy',
    '#ffc1c1': 'daughter, niece, grandson, little girl',
    '#eed5b7': 'friend, classmate, homie, companion, cousin',
    '#9b30ff': 'dom, hypnotist, owner',
    '#ffe1ff': 'sub, subject, pet, slave',
    '#c6e2ff': 'little, little one, kid, child, grandchild',
    '#ab82ff': 'parent, caretaker, caregiver, grandparent',
    '#ff3e96': 'partner, love, lover, mate',
    '#fffacd': 'cuddlebuddy',
  };

  const RelationshipMap = new Map(RelationshipIndex.map((name, i) => [name, i]));
  RelationshipMap.delete('unknown');
  const ColorIndex = new Array(RelationshipIndex.length);

  for (const [color, types] of Object.entries(RelationshipColors)) {
    for (const type of types.split(',')) {
      ColorIndex[RelationshipMap.get(type.trim())] = color;
    }
  }

  const Help = (message, commandBase) => {
    Reply(message, Helps.get(commandBase) || Helps.get('default')).catch(LogError);
  };

  function IsPremium(message) {
    let guild = message.guild;
    return guild == null ? false : PremiumGuilds.has(guild.id);
  }
  function IsDeveloper(message) {
    let guild = message.guild;
    return guild == null ? false : PremiumGuilds.get(guild.id) > 1;
  }

  /*async function SetRelationship(personId, famId, type, message) {
	let update = await User.updateOne({ "_id": personId, "fam.id": famId }, { 
		"fam.$.t": type
	});

	if(update.n !== 0) return;

	let premium = IsPremium(message);
	
	User.updateOne({ "_id": personId, "bloc": {"$ne": famId} }, { 
        "$push": {
            "fam": {
				"$each": [ { id: famId, t: type } ],
				"$slice": -(premium ? PremiumRelationshipLimit : RelationshipLimit)
			}
        }
    }, { upsert: true }).exec();
}*/

  async function SetRelationship(personId, famId, type, message) {
    let update = await User.updateOne(
      { _id: personId, 'fam.id': famId },
      {
        'fam.$.t': type,
      }
    );

    if (update.n !== 0) return;

    let premium = IsPremium(message);

    let push = {
      $push: {
        fam: {
          $each: [{ id: famId, t: type }],
          $slice: -(premium ? PremiumRelationshipLimit : RelationshipLimit),
        },
      },
    };

    update = await User.updateOne({ _id: personId, bloc: { $ne: famId } }, push);

    if (update.n !== 0) return;

    if (await User.exists({ _id: personId })) return;

    User.updateOne({ _id: personId }, push, { upsert: true }).exec();
  }

  var RelationshipRequests = new Map();
  var ShipRequests = new Map();
  var RelationshipRequestMessages = new Map();
  function DeleteRelationshipRequest(messageId, timeout) {
    let request = RelationshipRequestMessages.get(messageId);
    if (request === undefined) return;
    RelationshipRequestMessages.delete(messageId);
    if (timeout && request.m) request.m.delete().catch(() => {}); //ignore error
    if (request.s) ShipRequests.delete(request.im.author.id);
    else RelationshipRequests.delete(request.to + ':' + request.for);
    if (!timeout) clearTimeout(request.x);
  }

  async function Reply(message, content, noMention) {
    try {
      if (!noMention) {
        return await message.reply(content);
      } else {
        return await message.channel.send(content);
      }
    } catch (err) {
      if ((err.code === 50001 /*Missing Access*/ || err.code === 50013) /*Missing Permissions*/ && message.author != null && !message.author.bot) {
        await message.author.send(
          `I don't have permission to send messages in ${message.guild.name} <#${message.channel.id}>, please fix your server settings!\nThe original message was:`
        );
        await message.author.send(content);
      }
      throw err;
    }
  }

  function Delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
  async function React(message, emoji) {
    try {
      return await message.react(emoji);
    } catch (err) {
      if (err.code === 10008 /*Unknown Message*/) {
        await Delay(ReactionDelay);
        return await message.react(emoji);
      } else if (err.code === 50001 /*Missing Access*/ || err.code === 50013 /*Missing Permissions*/) {
        await Reply(message, "I don't have permission to react to messages, please fix your server settings!", message.author === Me);
      }
      throw err;
    }
  }
  async function AddYesNoReactions(message) {
    try {
      await React(message, YesEmoji);
      await Delay(ReactionDelay);
      await message.react(NoEmoji);
    } catch (err) {
      LogError(err);
    }
  }

  function ResolveRelationshipType(typeName, message, premium) {
    let type = RelationshipMap.get(typeName);
    if (type === undefined) {
      if (!premium) {
        let lowerTypeName = typeName.toLowerCase();
        if (lowerTypeName !== typeName) type = RelationshipMap.get(typeName.toLowerCase());
        if (type === undefined) Reply(message, `Sorry, ${typeName} relationship only works on a premium server (${Prefix}help premium)`).catch(LogError);
      } else type = typeName;
    }
    return type;
  }

  const RelationshipCommand = async (typeName, selfTypeName, message, commandBase) => {
    let mentions = message.mentions.users;
    if (mentions.size === 0) return Help(message, commandBase);

    let premium = IsPremium(message);

    let type = ResolveRelationshipType(typeName, message, premium);
    if (type == null) return;

    let userId = message.author.id;
    for (let [mentionId] of mentions) {
      if (mentionId !== userId) SetRelationship(userId, mentionId, type, message);
    }

    let replied = false;

    if (selfTypeName != null) {
      let selfType = ResolveRelationshipType(selfTypeName, message, premium);
      if (selfType == null) return;

      let members = message.channel.members;
      if (members != null) {
        let initiatorName = message.member ? message.member.displayName : message.author.username;
        for (let [mentionId] of mentions) {
          if (mentionId !== userId) {
            let member = members.get(mentionId);
            if (member !== undefined && !member.user.bot) {
              let requestId = userId + ':' + mentionId;
              if (RelationshipRequests.has(requestId)) {
                Reply(message, `Status updated, but you already have a relationship request open to ${member.displayName}`).catch(LogError);
                replied = true;
              } else {
                try {
                  let request = {};
                  RelationshipRequests.set(requestId, request);
                  let requestMessage = await Reply(
                    message,
                    `<@${mentionId}>, you are ${initiatorName}'s ${typeName} now!\nWould you like them to be your ${selfTypeName + (type === selfType ? ' too' : '')}?`,
                    true
                  );
                  AddYesNoReactions(requestMessage);
                  RelationshipRequestMessages.set(requestMessage.id, request);
                  request.m = requestMessage;
                  request.for = mentionId;
                  request.to = userId;
                  request.t = selfType;
                  request.tn = selfTypeName;
                  request.pn = typeName;
                  request.im = message;
                  request.x = setTimeout(() => DeleteRelationshipRequest(requestMessage.id, true), RequestTimeout);
                  replied = true;
                } catch (err) {
                  RelationshipRequests.delete(requestId);
                  LogError(err);
                }
              }
            }
          }
        }
      }
    }

    if (!replied) React(message, ConfirmEmoji).catch(LogError);
  };

  const MentionsRegex = /<@!?\d{16,20}>/g;
  const MentionsArrowsRegex = /\s*(?:<@!?\d{16,20}>|->)\s*/g;
  const CustomRelationshipCommand = (message, command, commandBase) => {
    command = command.substr(commandBase.length + 1).trim();

    let types = command.split(MentionsArrowsRegex).filter((x) => x != '');
    if (types.length === 0 || types.length > 2) return Help(message, commandBase);

    RelationshipCommand(types[0], types[1], message, commandBase);
  };

  const ShipCommand = async (message, command, commandBase) => {
    if (ShipRequests.has(message.author.id)) {
      Reply(message, 'Sorry but you already have a ship waiting to sail.').catch(LogError);
      return;
    }

    let mentions = message.mentions.users;
    if (mentions.size !== 2) return Help(message, commandBase);

    mentions = [...mentions.keys()];

    command = command.substr(commandBase.length + 1).trim();

    let types = command.split(MentionsArrowsRegex).filter((x) => x != '');
    if (types.length !== 2) {
      Reply(
        message,
        "Please specify the roles in the relationship in the command as:\nrole of the first person **->** role of the second person\nFor example: waifu -> husbando\nNote: they aren't server roles, just text"
      ).catch(LogError);
      return;
    }

    let premium = IsPremium(message);

    let type1Name = types[0];
    let type2Name = types[1];
    if (type1Name.length === 0 || type2Name.length === 0) return Help(message, commandBase);
    let type1 = ResolveRelationshipType(type1Name, message, premium);
    if (type1 == null) return;
    let type2 = ResolveRelationshipType(type2Name, message, premium);
    if (type2 == null) return;

    try {
      let request = { s: true };
      ShipRequests.set(message.author.id, request);
      let msg;
      if (type1 === type2) msg = `Wouldn't it be nice if you were eachother's ${type1Name}?`;
      else msg = `Would you like to be ${type1Name} and ${type2Name}? (<@${mentions[0]}> ${type1Name}, <@${mentions[1]}> ${type2Name})`;
      let requestMessage = await Reply(message, `<@${mentions[0]}> <@${mentions[1]}>, ${msg}`, true);
      AddYesNoReactions(requestMessage);
      RelationshipRequestMessages.set(requestMessage.id, request);
      request.m = requestMessage;
      request.for = mentions[0];
      request.to = mentions[1];
      request.t = type1;
      request.p = type2;
      request.tn = type1Name;
      request.pn = type2Name;
      request.im = message;
      request.x = setTimeout(() => DeleteRelationshipRequest(requestMessage.id, true), RequestTimeout);
    } catch (err) {
      ShipRequests.delete(message.author.id);
      LogError(err);
    }
  };

  function QueryDirectRelationships(userId) {
    return User.findById(userId, 'fam', { lean: true });
  }
  function QueryBlockedRelationships(userId) {
    return User.find({ bloc: userId }, '_id', { lean: true });
  }

  function QueryRelationshipTree(userId, depth, withDepth) {
    return withDepth
      ? User.aggregate([{ $match: { _id: Types.Long.fromString(userId) } }])
          .graphLookup({
            from: 'users',
            startWith: '$fam.id',
            connectFromField: 'fam.id',
            connectToField: '_id',
            as: 'conn',
            maxDepth: depth,
            depthField: 'd',
          })
          .project({
            fam: 1,
            tree: {
              ids: '$conn._id',
              fams: '$conn.fam',
              depths: '$conn.d',
            },
          })
      : User.aggregate([{ $match: { _id: Types.Long.fromString(userId) } }])
          .graphLookup({
            from: 'users',
            startWith: '$fam.id',
            connectFromField: 'fam.id',
            connectToField: '_id',
            as: 'conn',
            maxDepth: depth,
          })
          .project({
            fam: 1,
            tree: {
              ids: '$conn._id',
              fams: '$conn.fam',
            },
          });
  }

  async function GetRelationshipGroups(userId) {
    let query = await QueryDirectRelationships(userId);
    if (query == null) return null;
    let fam = query.fam;
    if (fam == null || fam.length === 0) return null;

    const relationshipMap = new Map();

    for (const relationship of fam) {
      /*if(relationship.id.startsWith('-')) //fix sign bit ... only needed from 2084
			relationship.id = BigInt.asUintN(64, BigInt(relationship.id)).toString();*/

      const collection = relationshipMap.get(relationship.t);
      if (collection === undefined) {
        relationshipMap.set(relationship.t, [relationship]);
      } else {
        collection.push(relationship);
      }
    }

    return relationshipMap;
  }

  function DotEscape(string) {
    return string.replace(/[\\"]/g, (m) => (m === '\\' ? '\\\\' : '\\"'));
  }

  function SetUserNode(userNamePromises, promiseCounter, node, userId, fontSizeMult = 1) {
    promiseCounter[0]++;
    userNamePromises.push(
      DiscordClient.users.fetch(userId).then((user) => {
        let name = user.username;
        node.set('label', DotEscape(name)).set('fontsize', (30 - name.length / 2) * fontSizeMult);
        promiseCounter[0]--;
      })
    );
  }

  async function GetRelationshipTree(userId, depth = 2, trimEnds) {
    let query;
    if (depth <= 0) {
      query = await QueryDirectRelationships(userId);
    } else {
      depth--;
      query = await QueryRelationshipTree(userId, depth, trimEnds);
      query = query[0];
    }
    if (query == null || query.fam == null || query.fam.length === 0) return null;

    let g = Graphviz.digraph('G');

    let rootId = query._id.toString();
    let root = g.addNode(rootId, { fillcolor: '#ff1111', shape: 'box' });
    let directSet = new Set([rootId]);
    let userNamePromises = [];
    let promiseCounter = [0];
    SetUserNode(userNamePromises, promiseCounter, root, rootId, 1.5);

    for (let relationship of query.fam) {
      let relationshipId = relationship.id.toString();
      let isIndexedRelationship = typeof relationship.t === 'number';
      let color = isIndexedRelationship ? ColorIndex[relationship.t] || '#f5d300' : '#f5d300';
      let node = g.addNode(relationshipId, { fillcolor: color });
      SetUserNode(userNamePromises, promiseCounter, node, relationshipId);
      if (isIndexedRelationship) g.addEdge(root, node, { color });
      else g.addEdge(root, node, { color, label: DotEscape(relationship.t) });
      directSet.add(relationshipId);
    }

    if (query.tree !== undefined) {
      let ids = query.tree.ids;
      let fams = query.tree.fams;
      let depths = query.tree.depths;
      let rootindex = -1;
      for (let i = ids.length - 1; i >= 0; i--) {
        let id = ids[i].toString();
        ids[i] = id;
        if (!directSet.delete(id)) {
          //no duplicates so we can delete, returns true if exists
          let node = g.addNode(id);
          SetUserNode(userNamePromises, promiseCounter, node, id);
        } else if (id === rootId) {
          rootindex = i;
        }
      }

      for (let i = ids.length - 1; i >= 0; i--) {
        if (i === rootindex) continue;
        let node = g.getNode(ids[i]);
        let trim = trimEnds && depths !== undefined && depths[i] === depth;
        for (let relationship of fams[i]) {
          let id = relationship.id.toString();
          let otherNode = g.getNode(id);
          if (otherNode == null) {
            if (trim) continue;
            otherNode = g.addNode(id);
            SetUserNode(userNamePromises, promiseCounter, otherNode, id);
          }

          if (typeof relationship.t === 'number') {
            let color = ColorIndex[relationship.t];
            if (color == null) g.addEdge(node, otherNode);
            else g.addEdge(node, otherNode, { color });
          } else {
            g.addEdge(node, otherNode, { label: DotEscape(relationship.t) });
          }
        }
      }
    }

    return [g, userNamePromises, promiseCounter];
  }

  const DirectRelationshipsCommand = async (message) => {
    let userId = (message.mentions.users.first() || message.author).id;

    try {
      let relationships = await GetRelationshipGroups(userId);
      if (relationships == null) {
        await Reply(message, (userId === message.author.id ? 'You' : 'They') + " don't have anyone, make sure to add some people~");
        return;
      }

      let fields = Array.from(relationships, ([name, relationships]) => ({
        //TODO fix overflows
        name: typeof name === 'number' ? RelationshipIndex[name] : name,
        value: `<@${relationships.map((x) => x.id).join('> <@')}>`,
      }));

      await Reply(message, {
        embeds: [
          {
            fields,
          },
        ],
      });
    } catch (err) {
      LogError(err);
    }
  };

  var ManagerCommands = new Map();
  var ManagerMsg = (msg) => {
    let commandId = msg[0];
    let result = msg[1];
    let resolve = ManagerCommands.get(commandId);
    if (resolve !== undefined) {
      resolve(result);
    } else {
      LogError(`Manager responded to an unknown command`);
    }
  };

  //const ManagerCommandPromise = (commandId) => new Promise((resolve) => ManagerCommands.set(commandId, resolve));
  var ManagerCommandCounter = 0;
  function ManagerCommand(type, ...args) {
    let commandId = ShardIdPrefix + (ManagerCommandCounter++).toString(32);
    let managerPromise = new Promise((resolve) => ManagerCommands.set(commandId, resolve));
    Shard.send([type, args, commandId]).catch(LogError);
    return managerPromise;
  }

  function ManagerVoidCommand(type, ...args) {
    Shard.send([type, args]).catch(LogError);
  }

  async function TakeRenderSlot(premium, message) {
    let [slot, reply] = await ManagerCommand('TakeRenderSlot', premium);
    if (reply) Reply(message, reply).catch(LogError);
    return slot !== 0 ? [slot] : null;
  }

  function FreeRenderSlot(slot) {
    if (slot[0] !== 0) {
      ManagerVoidCommand('FreeRenderSlot', slot[0]);
      slot[0] = 0;
    }
  }

  const TreeDepthRegex = /(?:[^\d]|^)(\d)(?:(\.[1-9])|[^\d]|$)/;
  const RelationshipGraphCommand = async (message, command, commandBase) => {
    let premium = IsPremium(message);
    let slot = await TakeRenderSlot(premium, message);
    if (slot == null) return;

    let user = message.mentions.users.first() || message.author;
    let userId = user.id;

    let depth;
    let trimTree = false;
    if (command.length !== commandBase.length) {
      command = command.substr(commandBase.length + 1);
      let depthMatch = TreeDepthRegex.exec(command);
      if (depthMatch != null) {
        depth = Math.min(parseInt(depthMatch[1]), 3);
        trimTree = depthMatch[2] === undefined;
      }
    }

    try {
      let result = await GetRelationshipTree(userId, depth, trimTree);
      if (result == null) {
        await Reply(message, (userId === message.author.id ? 'You' : 'They') + " don't have anyone, make sure to add some people~");
        FreeRenderSlot(slot);
        return;
      }

      let [graph, userNamePromises, promiseCounter] = result;
      let fetchedUsers = promiseCounter[0];
      let graphSize = userNamePromises.length;

      let delayNotification;
      let oldFetchStat = Statistics.fetch[fetchedUsers];
      let oldGraphStat = Statistics.graph[graphSize];
      let predictedWait = oldFetchStat + oldGraphStat;
      if (isNaN(predictedWait)) {
        delayNotification = Reply(message, `This might take a while`);
        delayNotification.catch(LogError);
      } else if (predictedWait > 5000) {
        let time;
        if (predictedWait > 58000) {
          let minutes = Math.round(predictedWait / 60000);
          time = minutes + (minutes === 1 ? ' minute' : 'minutes');
        } else {
          time = Math.round(predictedWait / 1000) + ' seconds';
        }
        delayNotification = Reply(message, `This might take about ${time}`);
        delayNotification.catch(LogError);
      }
      let waitStart = Date.now();

      await Promise.allSettled(userNamePromises);

      let now = Date.now();
      let waitTime = now - waitStart;
      let fetchStat;
      if (oldFetchStat != null) fetchStat = (oldFetchStat * StatisticsWeight + waitTime) / (StatisticsWeight + 1);
      else fetchStat = waitTime;

      Shard.broadcastEval((client, { fetchedUsers, fetchStat }) => (client.Statistics.fetch[fetchedUsers] = fetchStat), { context: { fetchedUsers, fetchStat } });

      waitStart = now;

      let fileName = `graphs/${message.id}.png`;
      graph.render(
        {
          path: process.env.GRAPHVIZ,
          type: 'png',
          use: 'sfdp',
          G: {
            bgcolor: 'transparent',
            rotation: 180,
          },
          N: {
            style: 'filled',
            fillcolor: '#09fbd3',
            fontname: 'Consolas',
            penwidth: 0,
          },
          E: {
            color: '#08f7fe',
            fontname: 'Tahoma',
            fontcolor: '#08f7fe',
            penwidth: 2,
            arrowsize: 0.5,
          },
        },
        fileName
      );
      let graphvizError = '';
      graphviz.stderr.on('data', (data) => {
        graphvizError += data;
      });
      graphviz.on('exit', async (code) => {
        if (code !== 0) {
          LogError({ 'GRAPHVIZ ERROR': graphvizError });
          await Reply(message, 'There was an error rendering the graph');
          FreeRenderSlot(slot);
          return;
        } else {
          if (delayNotification != null) {
            try {
              delayNotification = await delayNotification;
              delayNotification.delete().catch(LogError);
            } catch (err) {
              LogError(err);
            }
          }

          try {
            await Reply(message, { files: [{ attachment: fileName, name: `RelationshipBot_${user.username}.png` }] });

            let waitTime = Date.now() - waitStart;
            let newStat;
            if (oldGraphStat != null) newStat = (Statistics.graph[graphSize] * StatisticsWeight + waitTime) / (StatisticsWeight + 1);
            else newStat = waitTime;

            Shard.broadcastEval((client, { graphSize, newStat }) => (client.Statistics.graph[graphSize] = newStat), { context: { graphSize, newStat } });
            StatisticsChanged = true;
          } catch (err) {
            LogError(err);
          }

          Fs.unlink(fileName).catch(LogError);
        }
        FreeRenderSlot(slot);
      });
    } catch (err) {
      FreeRenderSlot(slot);
      LogError(err);
    }
  };

  function ShuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
      let j = Math.floor(Math.random() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
    }
  }

  function CdnDownloadSemaphoreWait() {
    return ManagerCommand('CdnDownloadSemaphoreWait');
  }

  function CdnDownloadSemaphoreRelease() {
    ManagerVoidCommand('CdnDownloadSemaphoreRelease');
  }

  const SharpOptions = { limitInputPixels: 4000000 };
  var PicNameRegex = /[\w-]+/;
  const PictureEditCommand = async (message, command, commandBase, pictureName) => {
    let imageCount = Images.anipics.length;
    if (imageCount === 0) return;

    let premium = IsPremium(message);
    let slot = await TakeRenderSlot(premium, message);
    if (slot == null) return;

    if (pictureName == null) {
      if (command.length !== commandBase.length) {
        command = command.substr(commandBase.length + 1);
        let match = PicNameRegex.exec(command.replace(MentionsRegex, ''));
        if (match != null) pictureName = Images.anipics[Images.animap.get(match[0].toLowerCase())];
      }
    }
    pictureName = pictureName || Images.anipics[Math.floor(Math.random() * imageCount)];

    try {
      let pictureData = JSON.parse(await Fs.readFile(`pics/anipics/metadata/${pictureName}.json`, 'utf8'));
      let layers = pictureData.l;
      if (typeof pictureData.l === 'string') layers = [layers];

      layers = layers.map((x) => Fs.readFile(`pics/anipics/layers/${x}`));

      let rects = pictureData.r;
      ShuffleArray(rects);

      let avatars = [];
      if (message.mentions.users.size !== 0) {
        let users = [...message.mentions.users.values()];
        if (users.length > rects.length) users = users.slice(0, rects.length);
        await Promise.all(
          users.map(async (user) => {
            await CdnDownloadSemaphoreWait();
            try {
              let avatar = await (await fetch(user.displayAvatarURL({ size: 256 }))).buffer();
              avatars.push([user, avatar]);
            } catch (err) {
              LogError(err);
              await Reply(message, "There was an error fetching one of the mentioned users' avatars");
            }
            CdnDownloadSemaphoreRelease();
          })
        );
      } else {
        let userId = message.author.id;
        let fam = [];
        if (rects.length > 1) {
          let query = await QueryDirectRelationships(userId);
          if (query != null && query.fam != null) {
            fam = query.fam.map((x) => x.id);
            ShuffleArray(fam);
          }
        }
        fam.unshift(userId);

        let requiredAvatars = Math.min(rects.length, fam.length);
        let triedAvatars = requiredAvatars;
        let lastTriedAvatars = 0;
        while (true) {
          await Promise.allSettled(
            fam.slice(lastTriedAvatars, triedAvatars).map(async (famId) => {
              await CdnDownloadSemaphoreWait();
              try {
                let user = await DiscordClient.users.fetch(famId);
                let avatar = await (await fetch(user.displayAvatarURL({ size: 256 }))).buffer();
                avatars.push([user, avatar]);
              } catch (err) {
                LogError(err);
                await Reply(message, "There was an error fetching one of the mentioned users' avatars");
              }
              CdnDownloadSemaphoreRelease();
            })
          );

          if (avatars.length === requiredAvatars || triedAvatars === fam.length) break;

          lastTriedAvatars = triedAvatars;
          triedAvatars = Math.min(lastTriedAvatars + (requiredAvatars - avatars.length), fam.length);
        }
      }

      let extraLayers = new Array(avatars.length);
      let avatarResizePromises = new Array(avatars.length);
      for (let i = 0; i < avatars.length; i++) {
        const avatar = avatars[i][1];
        const rect = rects[i];

        avatarResizePromises[i] = (async (i) => {
          let { data, info } = await Sharp(avatar).resize(rect[2], rect[3], { fit: 'fill' }).raw().toBuffer({ resolveWithObject: true });
          extraLayers[i] = {
            input: data,
            left: rect[0],
            top: rect[1],
            raw: {
              width: info.width,
              height: info.height,
              channels: info.channels,
            },
          };
        })(i);
      }
      await Promise.all(avatarResizePromises);

      for (let i = 1; i < layers.length; i++) {
        extraLayers.push({ input: await layers[i] });
      }

      //TODO REMOVE THIS STEP WHEN WEBP COMES TO IOS
      let { data, info } = await Sharp(await layers[0])
        .composite(extraLayers)
        .raw()
        .toBuffer({ resolveWithObject: true });
      let picture = Sharp(data, { raw: info });
      let width = info.width;
      let height = info.height;
      if (width > 800 || height > 600) {
        if (width / 800 > height / 600) {
          height = Math.round((800 / width) * height);
          width = 800;
        } else {
          width = Math.round((600 / height) * width);
          height = 600;
        }
        picture.resize(width, height);
      }
      picture = await picture.png().toBuffer();

      await Reply(message, { files: [{ attachment: picture, name: `RelationshipBot_${pictureName}_${avatars.map(([x]) => x.username).join('_')}.png` }] });
    } catch (err) {
      LogError(err);
    } finally {
      FreeRenderSlot(slot);
    }
  };

  async function FindPictureRectangles(imageBuffer) {
    let { data, info } = await Sharp(imageBuffer, SharpOptions).raw().toBuffer({ resolveWithObject: true });

    let channels = info.channels;
    let byteWidth = info.width * channels;

    const minPfpSize = 20;
    let xSearchEnd = info.width - minPfpSize;
    let ySearchEnd = info.height - minPfpSize;
    let spottedPixels = [];
    for (let yi = 0; yi < ySearchEnd; yi += minPfpSize) {
      let yStart = yi * byteWidth;
      for (let xi = 0; xi < xSearchEnd; xi += minPfpSize) {
        if ((data.readInt32LE(yStart + xi * channels) & 0x00ffffff) === 0x252220) spottedPixels.push((yi << 16) | xi);
      }
    }

    let spottedRects = [];
    for (const pixelCoords of spottedPixels) {
      let x = pixelCoords & 0xffff;
      let y = pixelCoords >> 16;
      let inRect = spottedRects.findIndex((rect) => rect[0] <= x && rect[1] <= y && rect[2] >= x && rect[3] >= y);
      if (inRect !== -1) continue;

      let rect = [];

      let yStart = y * byteWidth;
      let xi = x - 1;
      for (; xi >= 0 && (data.readInt32LE(yStart + xi * channels) & 0x00ffffff) === 0x252220; xi--);
      rect[0] = xi + 1;
      xi = x + 1;
      for (; xi < info.width && (data.readInt32LE(yStart + xi * channels) & 0x00ffffff) === 0x252220; xi++);
      rect[2] = xi - 1;

      let xOffset = x * channels;
      let yi = y - 1;
      for (; yi >= 0 && (data.readInt32LE(yi * byteWidth + xOffset) & 0x00ffffff) === 0x252220; yi--);
      rect[1] = yi + 1;
      yi = y + 1;
      for (; yi < info.height && (data.readInt32LE(yi * byteWidth + xOffset) & 0x00ffffff) === 0x252220; yi++);
      rect[3] = yi - 1;

      let w = rect[2] - rect[0] + 1;
      let h = rect[3] - rect[1] + 1;
      if (w >= minPfpSize && h >= minPfpSize) spottedRects.push(rect);
    }

    return spottedRects.map((x) => [x[0], x[1], x[2] - x[0] + 1, x[3] - x[1] + 1]);
  }
  function GetExtension(fileName) {
    let dotIndex = fileName.lastIndexOf('.');
    if (dotIndex === -1) return null;
    return fileName.substr(dotIndex + 1).toLowerCase();
  }
  const ImageExtensions = ['png', 'jpg', 'jpeg', 'jfif', 'gif', 'webp'];
  function ReadStream(stream) {
    return new Promise((resolve, reject) => {
      let chunks = [];
      stream.on('error', reject);
      stream.on('data', (data) => {
        chunks.push(data);
      });
      stream.on('end', () => {
        resolve(Buffer.concat(chunks));
      });
    });
  }

  const PictureUploadCommand = async (message, command, commandBase) => {
    if (!IsPremium(message)) {
      Reply(message, `Sorry but this is a limited command, visit our server (${Prefix}invite)`).catch(LogError);
      return;
    }

    let attachment = message.attachments.first();
    if (attachment === undefined) return Help(message, commandBase);

    let fileName = attachment.name;
    let dotIndex = fileName.lastIndexOf('.');
    if (dotIndex === -1) return Help(message, commandBase);
    let extension = fileName.substr(dotIndex + 1).toLowerCase();
    if (command.length !== commandBase.length) {
      fileName = command.substr(commandBase.length + 1).trimStart();
      if (/[^\w-]/.test(fileName)) {
        Reply(message, `Sorry but only the characters A-Z a-z 0-9 '_' and '-' are allowed.`).catch(LogError);
        return;
      }
      if (fileName.length > 50) {
        Reply(message, `That file name is too long.`).catch(LogError);
        return;
      }
    } else {
      fileName = fileName.substr(0, dotIndex);
      if (fileName.startsWith('SPOILER_')) fileName = fileName.substr(8);
    }
    let lowerFilename = fileName.toLowerCase();
    let existing = Images.animap.get(lowerFilename);
    if (existing !== undefined) {
      Reply(message, `There is already a file with the name \`${Images.anipics[existing]}\``).catch(LogError);
      return;
    }

    switch (extension) {
      case 'zip':
        try {
          await CdnDownloadSemaphoreWait();

          let buffer = await (await fetch(attachment.url)).buffer();

          let entries;
          try {
            let zipFile = await Yauzl.fromBuffer(buffer);
            entries = await zipFile.readEntries(10);
          } catch (err) {
            await Reply(message, "I couldn't open your zip file");
            return;
          }

          entries = entries.filter((x) => x.uncompressedSize < 10485760 /*10MiB*/ && ImageExtensions.indexOf(GetExtension(x.fileName)) !== -1);
          if (entries.length === 0) {
            await Reply(message, "Your zip didn't have any valid image files");
            return;
          }

          entries.sort((a, b) => a.fileName.localeCompare(b.fileName));

          let markerImage;
          let markerN;
          if (entries.length > 2) {
            markerImage = entries.splice(1, 1)[0];
            markerN = '2nd';
          } else {
            markerImage = entries[0];
            markerN = '1st';
          }

          buffer = await ReadStream(await markerImage.openReadStream());

          let spottedRects = await FindPictureRectangles(buffer);
          if (spottedRects.length === 0) {
            await Reply(message, `I couldn't find any marker rectangles on the ${markerN} image, please make sure they are #202225`);
            return;
          }

          let savedLayers = [];
          try {
            for (let i = 0; i < entries.length; i++) {
              const imageEntry = entries[i];

              let imageBuffer;
              if (imageEntry === markerImage) imageBuffer = buffer;
              else imageBuffer = await ReadStream(await imageEntry.openReadStream());
              let layerName = `${fileName}_${i}.webp`;
              await Sharp(imageBuffer, SharpOptions).webp({ quality: 90 }).toFile(`pics/anipics/layers/${layerName}`);
              savedLayers.push(layerName);
            }
          } catch (err) {
            savedLayers.forEach((x) => Fs.unlink(`pics/anipics/layers/${x}`).catch(() => {}));
            Reply(message, `Failed to save the files`).catch(LogError);
            return;
          }

          let metadata = {
            l: savedLayers,
            r: spottedRects,
            u: message.author.id,
          };
          if (message.guild) metadata.s = message.guild.id;
          await Fs.writeFile(`pics/anipics/metadata/${fileName}.json`, JSON.stringify(metadata));

          Shard.broadcastEval((client, { lowerFilename, fileName }) => client.Images.animap.set(lowerFilename, client.Images.anipics.push(fileName) - 1), {
            context: { lowerFilename, fileName },
          });

          Reply(message, `Successfully uploaded with filename \`${fileName}\``).catch(LogError);

          PictureEditCommand(message, command, commandBase, fileName);
        } catch (err) {
          LogError(err);
        } finally {
          CdnDownloadSemaphoreRelease();
        }

        break;
      case 'png':
      case 'jpg':
      case 'jpeg':
      case 'jfif':
      case 'gif':
      case 'webp':
        if (attachment.width > 2000 || attachment.height > 2000) break;

        try {
          await CdnDownloadSemaphoreWait();

          let buffer = await (await fetch(attachment.url)).buffer();

          let spottedRects = await FindPictureRectangles(buffer);
          if (spottedRects.length === 0) {
            await Reply(message, "I couldn't find any marker rectangles, please make sure they are #202225");
            return;
          }

          await Sharp(buffer, SharpOptions).webp({ quality: 90 }).toFile(`pics/anipics/layers/${fileName}_.webp`);

          let metadata = {
            l: `${fileName}_.webp`,
            r: spottedRects,
            u: message.author.id,
          };
          if (message.guild) metadata.s = message.guild.id;
          await Fs.writeFile(`pics/anipics/metadata/${fileName}.json`, JSON.stringify(metadata));
          Shard.broadcastEval((client, { lowerFilename, fileName }) => client.Images.animap.set(lowerFilename, client.Images.anipics.push(fileName) - 1), {
            context: { lowerFilename, fileName },
          });

          Reply(message, `Successfully uploaded with filename \`${fileName}\``).catch(LogError);

          PictureEditCommand(message, command, commandBase, fileName);
        } catch (err) {
          LogError(err);
        } finally {
          CdnDownloadSemaphoreRelease();
        }

        break;
      default:
        Help(message, commandBase);
        break;
    }
  };
  const PictureDeleteCommand = async (message, command, commandBase) => {
    if (!IsDeveloper(message)) return;

    try {
      let fileName = command.substr(commandBase.length + 1).trimStart();
      if (fileName.length === 0) {
        await Reply(message, `Please add a file name!`);
        return;
      }
      let lowerFilename = fileName.toLowerCase();
      let index = Images.animap.get(lowerFilename);
      if (index === undefined) {
        await Reply(message, `File not found by name: \`${fileName}\``);
        return;
      }
      fileName = Images.anipics.splice(index, 1)[0];
      Images.animap = new Map(Images.anipics.map((name, i) => [name.toLowerCase(), i]));
      Shard.broadcastEval(
        (client, { lowerFilename }) => {
          let index = client.Images.animap.get(lowerFilename);
          if (index !== undefined) {
            client.Images.anipics.splice(index, 1);
            client.Images.animap = new Map(client.Images.anipics.map((name, i) => [name.toLowerCase(), i]));
          }
        },
        { lowerFilename }
      );

      let pictureData = JSON.parse(await Fs.readFile(`pics/anipics/metadata/${fileName}.json`, 'utf8'));

      if (typeof pictureData.l === 'string') await Fs.unlink(`pics/anipics/layers/${pictureData.l}`);
      else await Promise.all(pictureData.l.map((x) => Fs.unlink(`pics/anipics/layers/${x}`)));

      await Fs.unlink(`pics/anipics/metadata/${fileName}.json`);

      await React(message, ConfirmRemoveEmoji);
    } catch (err) {
      LogError(err);
    }
  };
  const PictureUploaderCommand = async (message, command, commandBase) => {
    try {
      let fileName = command.substr(commandBase.length + 1).trimStart();
      if (fileName.length === 0) {
        await Reply(message, `Please add a file name!`);
        return;
      }
      let index = Images.animap.get(fileName.toLowerCase());
      if (index === undefined) {
        await Reply(message, `File not found by name: \`${fileName}\``);
        return;
      }

      fileName = Images.anipics[index];

      let pictureData = JSON.parse(await Fs.readFile(`pics/anipics/metadata/${fileName}.json`, 'utf8'));

      let msg = `\`${fileName}\` was uploaded by `;
      let user;
      try {
        user = await DiscordClient.users.fetch(pictureData.u);
      } catch (err) {}

      if (user != null) {
        msg += `${user.tag} (${pictureData.u})`;
      } else {
        msg += `userId ${pictureData.u}`;
      }

      if (pictureData.s != null) {
        let guild = DiscordClient.guilds.resolve(pictureData.s);
        if (guild != null) {
          msg += ` on the server ${guild.name} (${pictureData.s})`;
        } else {
          msg += ` on the server with the ID ${pictureData.s}`;
        }
      }

      await Reply(message, msg);
    } catch (err) {
      LogError(err);
    }
  };
  function JoinUntil(array, separator, limit, startIndex = 0) {
    let i = startIndex;
    if (i > array.length) return [array.length, ''];
    let result = array[i];
    let length = result.length;
    if (length > limit) return [i, ''];

    let separatorLength = separator.length;

    while (++i < array.length) {
      let newLength = length + separatorLength + array[i].length;
      if (newLength > limit) break;
      length = newLength;
    }

    return [i, array.slice(startIndex, i).join(separator)];
  }
  const PictureListCommand = async (message) => {
    let msgStart = `<@${message.author.id}>\n`;
    let limit = 2000 - msgStart.length;

    let pics = Images.anipics.map((x) => `\`${x}\``);

    try {
      let lastI = 0;
      let i = 0;
      do {
        let joined;
        [i, joined] = JoinUntil(pics, ' ', limit, i);
        if (i === lastI) break;
        lastI = i;
        await Reply(message, msgStart + joined, true);
      } while (i !== pics.length);
    } catch (err) {
      LogError(err);
    }
  };

  const IdsRegex = /\b\d{16,20}\b/g;
  const RemoveRelationshipCommand = (message, command, commandBase) => {
    let matches = command.match(IdsRegex); //commandbase isn't removed from the command here
    if (matches == null) return Help(message, commandBase);

    User.updateOne(
      { _id: message.author.id },
      {
        $pull: {
          fam: {
            id: { $in: matches },
          },
        },
      }
    ).exec();

    React(message, ConfirmRemoveEmoji).catch(LogError);
  };

  const BreakupRelationshipCommand = (message, command, commandBase) => {
    let matches = command.match(IdsRegex);
    if (matches == null || matches.length > 1) return Help(message, commandBase);

    User.updateOne(
      { _id: message.author.id },
      {
        $pull: {
          fam: {
            id: matches[0],
          },
        },
      }
    ).exec();

    User.updateOne(
      { _id: matches[0] },
      {
        $pull: {
          fam: {
            id: message.author.id,
          },
        },
      }
    ).exec();

    React(message, ConfirmRemoveEmoji).catch(LogError);
  };
  const BlockRelationshipCommand = (message, command, commandBase) => {
    let matches = command.match(IdsRegex);
    if (matches == null || matches.length > 1) return Help(message, commandBase);

    let userId = message.author.id;

    User.updateOne(
      { _id: userId },
      {
        $pull: {
          fam: {
            id: matches[0],
          },
        },
      }
    ).exec();

    User.updateOne(
      { _id: matches[0] },
      {
        $pull: {
          fam: {
            id: userId,
          },
        },
        $addToSet: {
          bloc: userId,
        },
      }
    ).exec();

    React(message, ConfirmRemoveEmoji).catch(LogError);
  };
  const UnBlockRelationshipCommand = (message, command, commandBase) => {
    let matches = command.match(IdsRegex);
    if (matches == null || matches.length > 1) return Help(message, commandBase);

    User.updateOne(
      { _id: matches[0] },
      {
        $pull: {
          bloc: message.author.id,
        },
      }
    ).exec();

    React(message, ConfirmEmoji).catch(LogError);
  };
  const ClearRelationshipBlocksCommand = (message) => {
    User.updateMany(
      { bloc: message.author.id },
      {
        $pull: {
          bloc: message.author.id,
        },
      }
    ).exec();

    React(message, ConfirmEmoji).catch(LogError);
  };
  const ListRelationshipBlocksCommand = async (message) => {
    let blocked = await QueryBlockedRelationships(message.author.id);

    Reply(message, { embed: { description: blocked.map((x) => `<@${x._id}>`).join(' ') } }).catch(LogError);
  };

  const RenderSlotsCommand = async (message) => {
    let [freeRenderSlotsUsed, premiumRenderSlotsUsed, cdnDownloadSemaphoreUsed] = await ManagerCommand('GetSlots');
    Reply(
      message,
      `\nFree rendering slots: **${freeRenderSlotsUsed}** used out of **${FreeRenderSlots}**\nPremium rendering slots: **${premiumRenderSlotsUsed}** used out of **${PremiumRenderSlots}**\nDownload slots: **${cdnDownloadSemaphoreUsed}** used out of **${CdnDownloadSlots}**`
    ).catch(LogError);
  };

  const Commands = new Map([
    ['help', HelpCommand],
    ['adopt', HelpCommand],
    ['marry', HelpCommand],
    ['marriage', HelpCommand],
    ['propose', HelpCommand],
    ['makeparent', HelpCommand],
    ['engage', HelpCommand],
    ['yuri', (message, _, commandBase) => RelationshipCommand('waifu', 'waifu', message, commandBase)],
    ['waifus', (message, _, commandBase) => RelationshipCommand('waifu', 'waifu', message, commandBase)],
    ['yaoi', (message, _, commandBase) => RelationshipCommand('husbando', 'husbando', message, commandBase)],
    ['waifu', (message, _, commandBase) => RelationshipCommand('waifu', 'husbando', message, commandBase)],
    ['husbando', (message, _, commandBase) => RelationshipCommand('husbando', 'waifu', message, commandBase)],
    ['husbandos', (message, _, commandBase) => RelationshipCommand('husbando', 'husbando', message, commandBase)],
    ['bro', (message, _, commandBase) => RelationshipCommand('bro', null, message, commandBase)],
    ['bros', (message, _, commandBase) => RelationshipCommand('bro', 'bro', message, commandBase)],
    ['sis', (message, _, commandBase) => RelationshipCommand('sis', null, message, commandBase)],
    ['sisxsis', (message, _, commandBase) => RelationshipCommand('sis', 'sis', message, commandBase)],
    ['broxsis', (message, _, commandBase) => RelationshipCommand('bro', 'sis', message, commandBase)],
    ['sisxbro', (message, _, commandBase) => RelationshipCommand('sis', 'bro', message, commandBase)],
    ['brother', (message, _, commandBase) => RelationshipCommand('brother', null, message, commandBase)],
    ['brothers', (message, _, commandBase) => RelationshipCommand('brother', 'brother', message, commandBase)],
    ['sister', (message, _, commandBase) => RelationshipCommand('sister', null, message, commandBase)],
    ['sisters', (message, _, commandBase) => RelationshipCommand('sister', 'sister', message, commandBase)],
    ['brothersister', (message, _, commandBase) => RelationshipCommand('brother', 'sister', message, commandBase)],
    ['brotherxsister', (message, _, commandBase) => RelationshipCommand('brother', 'sister', message, commandBase)],
    ['sisterbrother', (message, _, commandBase) => RelationshipCommand('sister', 'brother', message, commandBase)],
    ['sisterxbrother', (message, _, commandBase) => RelationshipCommand('sister', 'brother', message, commandBase)],
    ['grandpa', (message, _, commandBase) => RelationshipCommand('grandpa', null, message, commandBase)],
    ['grandma', (message, _, commandBase) => RelationshipCommand('grandma', null, message, commandBase)],
    ['oniichan', (message, _, commandBase) => RelationshipCommand('oniichan', null, message, commandBase)],
    ['oniisan', (message, _, commandBase) => RelationshipCommand('oniisan', null, message, commandBase)],
    ['oneechan', (message, _, commandBase) => RelationshipCommand('oneechan', null, message, commandBase)],
    ['oneesan', (message, _, commandBase) => RelationshipCommand('oneesan', null, message, commandBase)],
    ['friend', (message, _, commandBase) => RelationshipCommand('friend', null, message, commandBase)],
    ['friends', (message, _, commandBase) => RelationshipCommand('friend', 'friend', message, commandBase)],
    ['classmate', (message, _, commandBase) => RelationshipCommand('classmate', 'classmate', message, commandBase)],
    ['classmates', (message, _, commandBase) => RelationshipCommand('classmate', 'classmate', message, commandBase)],
    ['master', (message, _, commandBase) => RelationshipCommand('master', null, message, commandBase)],
    ['mistress', (message, _, commandBase) => RelationshipCommand('mistress', null, message, commandBase)],
    ['owner', (message, _, commandBase) => RelationshipCommand('owner', null, message, commandBase)],
    ['pet', (message, _, commandBase) => RelationshipCommand('pet', null, message, commandBase)],
    ['slave', (message, _, commandBase) => RelationshipCommand('slave', null, message, commandBase)],
    ['homie', (message, _, commandBase) => RelationshipCommand('homie', 'homie', message, commandBase)],
    ['homies', (message, _, commandBase) => RelationshipCommand('homie', 'homie', message, commandBase)],
    ['uncle', (message, _, commandBase) => RelationshipCommand('uncle', null, message, commandBase)],
    ['auntie', (message, _, commandBase) => RelationshipCommand('auntie', null, message, commandBase)],
    ['son', (message, _, commandBase) => RelationshipCommand('son', null, message, commandBase)],
    ['daughter', (message, _, commandBase) => RelationshipCommand('daughter', null, message, commandBase)],
    ['child', (message, _, commandBase) => RelationshipCommand('child', 'parent', message, commandBase)],
    ['niece', (message, _, commandBase) => RelationshipCommand('niece', null, message, commandBase)],
    ['nephew', (message, _, commandBase) => RelationshipCommand('nephew', null, message, commandBase)],
    ['cousin', (message, _, commandBase) => RelationshipCommand('cousin', 'cousin', message, commandBase)],
    ['cousins', (message, _, commandBase) => RelationshipCommand('cousin', 'cousin', message, commandBase)],
    ['grandson', (message, _, commandBase) => RelationshipCommand('grandson', null, message, commandBase)],
    ['granddaughter', (message, _, commandBase) => RelationshipCommand('granddaughter', null, message, commandBase)],
    ['grandchild', (message, _, commandBase) => RelationshipCommand('grandchild', 'grandparent', message, commandBase)],
    ['grandparent', (message, _, commandBase) => RelationshipCommand('grandparent', 'grandchild', message, commandBase)],
    ['parent', (message, _, commandBase) => RelationshipCommand('parent', 'child', message, commandBase)],
    ['partner', (message, _, commandBase) => RelationshipCommand('partner', 'partner', message, commandBase)],
    ['partners', (message, _, commandBase) => RelationshipCommand('partner', 'partner', message, commandBase)],
    ['little', (message, _, commandBase) => RelationshipCommand('little', 'caregiver', message, commandBase)],
    ['littleone', (message, _, commandBase) => RelationshipCommand('little one', null, message, commandBase)],
    ['little_one', (message, _, commandBase) => RelationshipCommand('little one', null, message, commandBase)],
    ['kid', (message, _, commandBase) => RelationshipCommand('kid', null, message, commandBase)],
    ['love', (message, _, commandBase) => RelationshipCommand('love', null, message, commandBase)],
    ['loves', (message, _, commandBase) => RelationshipCommand('love', 'love', message, commandBase)],
    ['lover', (message, _, commandBase) => RelationshipCommand('lover', null, message, commandBase)],
    ['lovers', (message, _, commandBase) => RelationshipCommand('lover', 'lover', message, commandBase)],
    ['companion', (message, _, commandBase) => RelationshipCommand('companion', 'companion', message, commandBase)],
    ['companions', (message, _, commandBase) => RelationshipCommand('companion', 'companion', message, commandBase)],
    ['daddy', (message, _, commandBase) => RelationshipCommand('daddy', null, message, commandBase)],
    ['mommy', (message, _, commandBase) => RelationshipCommand('mommy', null, message, commandBase)],
    ['littlegirl', (message, _, commandBase) => RelationshipCommand('little girl', null, message, commandBase)],
    ['little_girl', (message, _, commandBase) => RelationshipCommand('little girl', null, message, commandBase)],
    ['littleboy', (message, _, commandBase) => RelationshipCommand('little boy', null, message, commandBase)],
    ['little_boy', (message, _, commandBase) => RelationshipCommand('little boy', null, message, commandBase)],
    ['papa', (message, _, commandBase) => RelationshipCommand('papa', null, message, commandBase)],
    ['mama', (message, _, commandBase) => RelationshipCommand('mama', null, message, commandBase)],
    ['dom', (message, _, commandBase) => RelationshipCommand('dom', 'sub', message, commandBase)],
    ['sub', (message, _, commandBase) => RelationshipCommand('sub', 'dom', message, commandBase)],
    ['hypnotist', (message, _, commandBase) => RelationshipCommand('hypnotist', 'subject', message, commandBase)],
    ['subject', (message, _, commandBase) => RelationshipCommand('subject', null, message, commandBase)],
    ['hypnosubject', (message, _, commandBase) => RelationshipCommand('subject', 'hypnotist', message, commandBase)],
    ['caretaker', (message, _, commandBase) => RelationshipCommand('caretaker', null, message, commandBase)],
    ['caregiver', (message, _, commandBase) => RelationshipCommand('caregiver', null, message, commandBase)],
    ['mate', (message, _, commandBase) => RelationshipCommand('mate', 'mate', message, commandBase)],
    ['dad', (message, _, commandBase) => RelationshipCommand('dad', null, message, commandBase)],
    ['mom', (message, _, commandBase) => RelationshipCommand('mom', null, message, commandBase)],
    ['cuddlebuddy', (message, _, commandBase) => RelationshipCommand('cuddlebuddy', 'cuddlebuddy', message, commandBase)],
    ['cuddle_buddy', (message, _, commandBase) => RelationshipCommand('cuddlebuddy', 'cuddlebuddy', message, commandBase)],
    ['fam', DirectRelationshipsCommand],
    ['remove', RemoveRelationshipCommand],
    ['delete', RemoveRelationshipCommand],
    ['tree', RelationshipGraphCommand],
    ['familytree', RelationshipGraphCommand],
    ['graph', RelationshipGraphCommand],
    ['slots', RenderSlotsCommand],
    ['breakup', BreakupRelationshipCommand],
    ['reject', BreakupRelationshipCommand],
    ['disown', BreakupRelationshipCommand],
    ['divorce', BreakupRelationshipCommand],
    ['relationship', CustomRelationshipCommand],
    ['ship', ShipCommand],
    ['reloadpremium', ReloadPremiumCommand],
    ['reloadhelp', ReloadHelpCommand],
    ['anipic', PictureEditCommand],
    ['anipick', PictureEditCommand],
    ['uploadanipic', PictureUploadCommand],
    ['uplaodanipic', PictureUploadCommand],
    ['anipicuploader', PictureUploaderCommand],
    ['anipicuplaoder', PictureUploaderCommand],
    ['deleteanipic', PictureDeleteCommand],
    ['listanipic', PictureListCommand],
    ['listanipics', PictureListCommand],
    ['reloadpics', ReloadImagesCommand],
    ['block', BlockRelationshipCommand],
    ['unblock', UnBlockRelationshipCommand],
    ['clearblockeds', ClearRelationshipBlocksCommand],
    ['listblockeds', ListRelationshipBlocksCommand],
    ['setpresence', SetPresenceCommand],
    ['errorchannel', SetErrorChannelCommand],
    ['gc', GarbageCollectCommand],
    ['mem', MemoryCommand],
    ['addpremium', AddPremiumCommand],
    ['removepremium', RemovePremiumCommand],
    ['listpremium', ListPremiumCommand],
    ['invite', (message) => Help(message, 'invite')],
  ]);

  var RateMeasurements = new Map();
  const ClearRate = (id) => {
    RateMeasurements.delete(id);
  };
  DiscordClient.on('messageCreate', (message) => {
    let author = message.author;
    if (author == null || author.bot) return;

    let content = message.content;
    //if(content.length < MinCommandLength) return;

    if (content.startsWith(Prefix)) {
      let measurement = RateMeasurements.get(author.id);
      if (measurement === undefined) {
        measurement = { w: 1 };
        RateMeasurements.set(author.id, measurement);
        measurement.x = setTimeout(ClearRate, RateMeasureTime, author.id);
      } else if (measurement.w > RateThresholdWeight) {
        if (!measurement.p) {
          LogError(`${author.username} (${author.id}) hit the ratelimit` + (message.guild != null ? ` on ${message.guild.name} (${message.guild.id})` : ''));
          Reply(message, 'Please wait a few minutes before using commands again').catch(LogError);
          measurement.p = true;
          clearTimeout(measurement.x);
          measurement.x = setTimeout(ClearRate, RatelimitTime, author.id);
        }
        return;
      } else measurement.w++;

      let command = content.substr(PrefixLength).trimStart();
      let commandBaseEnd = command.indexOf(' ');
      let commandBase = commandBaseEnd <= 0 ? command : command.substr(0, commandBaseEnd);
      commandBase = commandBase.toLowerCase();
      let commandHandler = Commands.get(commandBase);
      if (commandHandler !== undefined) {
        commandHandler(message, command, commandBase);
        if (commandHandler === RelationshipGraphCommand) measurement.w += 2;
        else if (commandHandler === PictureEditCommand) measurement.w += 1;
      }
    }
  });

  DiscordClient.on('messageReactionAdd', (reaction, user) => {
    if (user.bot) return;
    let request = RelationshipRequestMessages.get(reaction.message.id);
    if (request === undefined) return;

    if (request.s) {
      //it's a ship
      let userId = user.id;
      if (userId !== request.for && userId !== request.to) return;

      let emojiName = reaction.emoji.name;
      if (emojiName === YesEmoji) {
        let message = reaction.message;
        if (userId === request.for) request.fora = true;
        else request.toa = true;
        if (!request.fora || !request.toa) return;

        SetRelationship(request.for, request.to, request.t, request.im);
        SetRelationship(request.to, request.for, request.p, request.im);
        let description = request.tn === request.pn ? "eachother's " + request.tn : request.pn + ' and ' + request.tn;
        message.edit(`<@${request.to}> <@${request.for}>, congratulations! You are now ${description}!`).catch(LogError);
        message.reactions.removeAll().catch(LogError);
        DeleteRelationshipRequest(message.id);
      } else if (emojiName === NoEmoji) {
        let message = reaction.message;
        message.edit(`I'm sorry <@${request.im.author.id}>, they can't be shipped.`).catch(LogError);
        message.reactions.removeAll().catch(LogError);
        DeleteRelationshipRequest(message.id);
      }
    } else {
      // Clear spam reactions
      if (user.id !== request.for) {
        reaction.remove().catch(LogError);
        return;
      }

      let emojiName = reaction.emoji.name;
      if (emojiName === YesEmoji) {
        let message = reaction.message;
        SetRelationship(request.for, request.to, request.t, request.im);
        let description = request.tn === request.pn ? "eachother's " + request.tn : request.tn + ' and ' + request.pn;
        message.edit(`<@${request.for}>, congratulations! You are now ${description}!`).catch(LogError);
        message.reactions.removeAll().catch(LogError);
        DeleteRelationshipRequest(message.id);
      } else if (emojiName === NoEmoji) {
        let message = reaction.message;
        message.edit(`I'm sorry <@${request.to}>, your relationship remains one sided.`).catch(LogError);
        message.reactions.removeAll().catch(LogError);
        DeleteRelationshipRequest(message.id);
      }
    }
  });

  Object.assign(DiscordClient, { LogError, ManagerMsg, PremiumGuilds, Statistics, Images });

  var StatisticsChanged = false;
  setInterval(() => {
    if (StatisticsChanged) {
      StatisticsChanged = false;
      Fs.writeFile('statistics.json', JSON.stringify(Statistics, null, '\t')).catch(LogError);
    }
  }, StatisticsSaveInterval);

  //Make sure there is no leftover junk
  Fs.readdir('graphs').then((files) => files.map((filename) => Fs.unlink(`graphs/${filename}`)));

  const dbl = new (require('dblapi.js'))(process.env.DBLTOKEN, DiscordClient);
  DiscordClient.login(process.env.TOKEN);

  // Restart function
  async function restartBot() {
    LogError('Restarting bot...');
    await DiscordClient.destroy();
    await DiscordClient.login(process.env.TOKEN);
    LogError('Bot restarted');
  }

  // If detect ratelimit, ignore it
  DiscordClient.on('rateLimit', (rateLimitInfo) => {
    LogError(`Ratelimit hit: ${rateLimitInfo.path}, retry after ${rateLimitInfo.timeout}ms`);
  });

  // If a shard times out, retry the shard
  DiscordClient.on('shardDisconnect', (event) => {
    LogError(`Shard ${event.shard.id} disconnected: ${event.reason}`);
    setTimeout(() => {
      restartBot();
    }, 5000);
  });

  // if sharding still in progress, ignore
  DiscordClient.on('shardPreReady', (event) => {
    LogError(`Shard preready: ${event.shard.id}`);
  });
})();
